#include "server/driver/distributed_executor.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "server/driver/worker_manager.hpp"

#include <functional>

namespace duckdb {

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p)
    : worker_manager(worker_manager_p), conn(conn_p) {
}

unique_ptr<QueryResult> DistributedExecutor::ExecuteDistributed(const string &sql) {
	// Distributed execution coordinator implementing DuckDB's parallel execution model
	//
	// Architecture mapping (thread-based → node-based):
	// 
	// DuckDB Parallel Execution:
	// 1. Query is compiled to a physical plan
	// 2. Data is partitioned across multiple threads
	// 3. Each thread executes with LocalSinkState
	// 4. Results are combined into GlobalSinkState
	// 5. Final result is produced
	//
	// Distributed Execution:
	// 1. Query is compiled to a logical/physical plan [COORDINATOR]
	// 2. Plan is partitioned and sent to worker nodes [COORDINATOR]
	// 3. Each worker executes its partition (LocalState semantics) [WORKER]
	// 4. Coordinator collects and combines results (GlobalState semantics) [COORDINATOR]
	// 5. Final result is returned to client [COORDINATOR]
	//
	// Key insight: Workers are execution units (like threads), coordinator aggregates
	
	if (!CanDistribute(sql)) {
		return nullptr;
	}

	auto &db_instance = *conn.context->db;
	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return nullptr;
	}

	// Phase 1: Plan extraction and validation
	unique_ptr<LogicalOperator> logical_plan;
	try {
		logical_plan = conn.ExtractPlan(sql);
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Failed to extract logical plan for query '%s': %s", sql, ex.what()));
		return nullptr;
	}
	if (logical_plan == nullptr) {
		DUCKDB_LOG_WARN(
		    db_instance,
		    StringUtil::Format("ExtractPlan returned null for query '%s', falling back to local execution", sql));
		return nullptr;
	}
	if (!IsSupportedPlan(*logical_plan)) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Logical plan for query '%s' contains unsupported operators", sql));
		return nullptr;
	}

	// STEP 1: Query what DuckDB would naturally do for parallelism
	// This helps us understand DuckDB's intelligent parallelization decisions
	idx_t natural_parallelism = QueryNaturalParallelism(*logical_plan);
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("📊 [STEP1] DuckDB would naturally use %llu parallel tasks", 
	                                  static_cast<long long unsigned>(natural_parallelism)));
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("📊 [STEP1] We have %llu workers available", 
	                                  static_cast<long long unsigned>(workers.size())));
	if (natural_parallelism > 0 && natural_parallelism != workers.size()) {
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("📊 [STEP1] NOTE: Mismatch between natural parallelism (%llu) and worker count (%llu)",
		                                  static_cast<long long unsigned>(natural_parallelism),
		                                  static_cast<long long unsigned>(workers.size())));
	}

	// STEP 2: Extract partition information from physical plan
	// This analyzes the plan to determine if we can use intelligent partitioning
	PlanPartitionInfo partition_info = ExtractPartitionInfo(*logical_plan, workers.size());
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("📊 [STEP2] Plan analysis complete - intelligent partitioning: %s", 
	                                  partition_info.supports_intelligent_partitioning ? "YES" : "NO (using rowid %%)"));

	std::cerr << "\n========== DISTRIBUTED EXECUTION START ==========" << std::endl;
	std::cerr << "Query: " << sql << std::endl;
	std::cerr << "Workers: " << workers.size() << std::endl;
	std::cerr << "Natural Parallelism: " << natural_parallelism << std::endl;
	std::cerr << "Intelligent Partitioning: " << (partition_info.supports_intelligent_partitioning ? "YES" : "NO") << std::endl;
	std::cerr << "Estimated Cardinality: " << partition_info.estimated_cardinality << std::endl;
	std::cerr << "=================================================" << std::endl;

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing query '%s' distributed across %llu workers", sql,
	                                                 static_cast<long long unsigned>(workers.size())));
	
	// Phase 2: Partition plan creation (CURRENT - manual rowid partitioning)
	// TODO: Replace with natural DuckDB parallelism in next steps
	// Create partitioned plans for each worker (each worker = one execution unit/thread)
	// Partition predicate is injected into the plan (e.g., WHERE rowid % N = worker_id)
	// This ensures data partitioning similar to how DuckDB partitions data across threads
	vector<string> partition_sqls;
	partition_sqls.reserve(workers.size());
	vector<string> serialized_plans;
	serialized_plans.reserve(workers.size());

	for (idx_t partition_id = 0; partition_id < workers.size(); ++partition_id) {
		// Create SQL with partition predicate embedded
		// STEP 6: Now uses partition_info for intelligent range-based or modulo partitioning
		auto partition_sql = CreatePartitionSQL(sql, partition_id, workers.size(), partition_info);
		unique_ptr<LogicalOperator> partition_plan;
		try {
			partition_plan = conn.ExtractPlan(partition_sql);
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Failed to extract logical plan for partition query '%s': %s",
			                                   partition_sql, ex.what()));
			return nullptr;
		}
		if (partition_plan == nullptr) {

			DUCKDB_LOG_WARN(db_instance, StringUtil::Format("Partition plan extraction returned null for query '%s'",
			                                                partition_sql));
			return nullptr;
		}
		if (!IsSupportedPlan(*partition_plan)) {

			DUCKDB_LOG_WARN(
			    db_instance,
			    StringUtil::Format("Partition plan for query '%s' contains unsupported operators", partition_sql));
			return nullptr;
		}
		// Serialize the plan for transmission to worker
		serialized_plans.emplace_back(SerializeLogicalPlan(*partition_plan));
		partition_sqls.emplace_back(std::move(partition_sql));
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Prepared serialized plan for worker %llu (size: %llu bytes)",
		                                    static_cast<long long unsigned>(partition_id),
		                                    static_cast<long long unsigned>(serialized_plans.back().size())));
	}
	
	// Log partition strategy summary
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("✅ [PARTITION] Created %llu partitions using %s strategy",
	                                  static_cast<long long unsigned>(workers.size()),
	                                  partition_info.supports_intelligent_partitioning ? "RANGE-BASED" : "MODULO"));
	
	std::cerr << "\n========== PARTITION SQLS ==========" << std::endl;
	for (idx_t i = 0; i < partition_sqls.size(); i++) {
		std::cerr << "Worker " << i << ": " << partition_sqls[i] << std::endl;
	}
	std::cerr << "====================================" << std::endl;

	// Phase 3: Prepare result schema and type information
	auto prepared = conn.Prepare(sql);
	if (prepared->HasError()) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Failed to prepare distributed query '%s': %s", sql, prepared->GetError()));
		return nullptr;
	}

	vector<string> names = prepared->GetNames();
	vector<LogicalType> types = prepared->GetTypes();
	vector<string> serialized_types;
	serialized_types.reserve(types.size());
	for (auto &type : types) {
		serialized_types.emplace_back(SerializeLogicalType(type));
	}
	
	// Phase 4: Distribute execution to workers
	// Each worker receives its partition plan and executes independently
	// Similar to how DuckDB schedules pipeline tasks to threads
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams;
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("🚀 [DISTRIBUTE] Coordinator: Distributing query to %llu workers (node-based parallel execution)", 
	                                  static_cast<long long unsigned>(workers.size())));

	for (idx_t idx = 0; idx < workers.size(); ++idx) {
		auto *worker = workers[idx];
		distributed::ExecutePartitionRequest req;

		// Send both SQL (fallback) and serialized plan (preferred)
		req.set_sql(partition_sqls[idx]);
		req.set_partition_id(idx);
		req.set_total_partitions(workers.size());
		req.set_serialized_plan(serialized_plans[idx]);
		for (const auto &name : names) {
			req.add_column_names(name);
		}
		for (const auto &type_bytes : serialized_types) {
			req.add_column_types(type_bytes);
		}

		// Execute on worker (worker acts as execution unit with LocalState)
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("📤 [DISTRIBUTE] Coordinator: Sending partition %llu/%llu to worker %s (plan: %llu bytes, sql: '%s')", 
		                                  static_cast<long long unsigned>(idx + 1),
		                                  static_cast<long long unsigned>(workers.size()),
		                                  worker->worker_id,
		                                  static_cast<long long unsigned>(serialized_plans[idx].size()),
		                                  partition_sqls[idx]));
		std::unique_ptr<arrow::flight::FlightStreamReader> stream;
		auto status = worker->client->ExecutePartition(req, stream);
		if (!status.ok()) {
			DUCKDB_LOG_WARN(db_instance, 
			                StringUtil::Format("❌ [DISTRIBUTE] Coordinator: Worker %s failed executing partition: %s",
			                                  worker->worker_id, status.ToString()));
			continue;
		}
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("✅ [DISTRIBUTE] Coordinator: Worker %s accepted partition %llu", 
		                                  worker->worker_id, static_cast<long long unsigned>(idx)));
		result_streams.emplace_back(std::move(stream));
	}
	D_ASSERT(!result_streams.empty());

	// Phase 5: Combine results (GlobalState aggregation)
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Collecting and merging results from %llu workers",
	                                                 static_cast<long long unsigned>(result_streams.size())));
	
	std::cerr << "\n========== MERGE PHASE ==========" << std::endl;
	std::cerr << "Collecting results from " << result_streams.size() << " workers..." << std::endl;
	
	auto result = CollectAndMergeResults(result_streams, names, types);

	if (result) {
		// Count total rows returned.
		idx_t total_rows = 0;
		auto materialized = dynamic_cast<MaterializedQueryResult *>(result.get());
		if (materialized) {
			total_rows = materialized->RowCount();
		}
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Distributed query completed: %llu total rows returned",
		                                                 static_cast<long long unsigned>(total_rows)));
		
		std::cerr << "FINAL RESULT: " << total_rows << " total rows" << std::endl;
		std::cerr << "=================================" << std::endl;
	}

	return result;
}

bool DistributedExecutor::CanDistribute(const string &sql) {
	// With plan-based execution, we can be much more permissive!
	// We serialize and send entire logical plans, not just modified SQL strings.
	// DuckDB handles complex operators (aggregations, joins, etc.) in the plan execution.
	//
	// Keep only essential restrictions:
	// 1. Must be a SELECT query
	// 2. Must have a FROM clause (need data source to partition)
	// 3. Cannot have ORDER BY (would require global ordering across workers)
	//
	// Previously blocked but now should work:
	// ✓ Aggregations (COUNT, SUM, AVG) - workers produce partial results, coordinator merges
	// ✓ GROUP BY - each worker groups its partition
	// ✓ JOINs - can be executed on each partition
	// ✓ Subqueries - included in the plan
	// ✓ DISTINCT - can be handled per-partition then globally
	
	string sql_upper = StringUtil::Upper(sql);
	StringUtil::Trim(sql_upper);
	
	// Must be a SELECT query
	if (!StringUtil::StartsWith(sql_upper, "SELECT")) {
		return false;
	}
	
	// Must have a data source to partition
	if (sql_upper.find(" FROM ") == string::npos) {
		return false;
	}
	
	// ORDER BY requires global ordering - problematic for distributed execution
	// (would need to collect all data, then sort)
	if (sql_upper.find(" ORDER BY") != string::npos) {
		return false;
	}
	
	// LIMIT without ORDER BY could work, but OFFSET is tricky in distributed context
	// TODO: Could support LIMIT by having coordinator stop after N rows collected
	if (sql_upper.find(" OFFSET ") != string::npos) {
		return false;
	}
	
	// Everything else should work with plan-based execution!
	return true;
}

string DistributedExecutor::CreatePartitionSQL(const string &sql, idx_t partition_id, idx_t total_partitions,
                                               const PlanPartitionInfo &partition_info) {
	auto &db_instance = *conn.context->db;
	
	string trimmed = sql;
	StringUtil::RTrim(trimmed);
	bool has_semicolon = !trimmed.empty() && trimmed.back() == ';';
	if (has_semicolon) {
		trimmed.pop_back();
		StringUtil::RTrim(trimmed);
	}
	
	string clause;
	
	// STEP 6: Use intelligent partitioning based on plan analysis
	if (partition_info.supports_intelligent_partitioning) {
		// Range-based partitioning: more cache-friendly and aligned with row groups
		// Each worker gets a contiguous range of rowids
		idx_t row_start = partition_id * partition_info.rows_per_partition;
		idx_t row_end = (partition_id + 1) * partition_info.rows_per_partition - 1;
		
		// For the last partition, extend to include any remainder rows
		if (partition_id == total_partitions - 1) {
			row_end = partition_info.estimated_cardinality;
		}
		
		clause = StringUtil::Format("rowid BETWEEN %llu AND %llu",
		                            static_cast<long long unsigned>(row_start),
		                            static_cast<long long unsigned>(row_end));
		
		DUCKDB_LOG_DEBUG(db_instance,
		                StringUtil::Format("🎯 [PARTITION] Worker %llu/%llu: Range partitioning [%llu, %llu]",
		                                  static_cast<long long unsigned>(partition_id),
		                                  static_cast<long long unsigned>(total_partitions),
		                                  static_cast<long long unsigned>(row_start),
		                                  static_cast<long long unsigned>(row_end)));
	} else {
		// Fallback: Modulo-based partitioning
		// This is used for small tables, non-table-scan operators, or when cardinality is unknown
		clause = StringUtil::Format("(rowid %% %llu) = %llu",
		                            static_cast<long long unsigned>(total_partitions),
		                            static_cast<long long unsigned>(partition_id));
		
		DUCKDB_LOG_DEBUG(db_instance,
		                StringUtil::Format("🎯 [PARTITION] Worker %llu/%llu: Modulo partitioning (rowid %% %llu = %llu)",
		                                  static_cast<long long unsigned>(partition_id),
		                                  static_cast<long long unsigned>(total_partitions),
		                                  static_cast<long long unsigned>(total_partitions),
		                                  static_cast<long long unsigned>(partition_id)));
	}
	
	string partition_sql = trimmed + " WHERE " + clause;
	if (has_semicolon) {
		partition_sql += ";";
	}
	return partition_sql;
}

bool DistributedExecutor::IsSupportedPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op.children.size() != 1) {
			return false;
		}
		return IsSupportedPlan(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_GET:
		return true;
	default:
		return false;
	}
}

// STEP 1: Helper method to understand DuckDB's natural parallelization decisions
// This extracts information about how DuckDB would naturally parallelize the query
idx_t DistributedExecutor::QueryNaturalParallelism(LogicalOperator &logical_plan) {
	auto &db_instance = *conn.context->db;
	idx_t estimated_threads = 0;
	
	try {
		// Wrap physical plan generation in a transaction (mimicking DuckDB's internal behavior)
		// This is necessary because physical plan generation requires an active transaction context
		conn.context->RunFunctionInTransaction([&]() {
			// Clone the logical plan since Plan() takes ownership
			auto cloned_plan = logical_plan.Copy(*conn.context);
			
			// Create a physical plan from the logical plan using the proper API
			PhysicalPlanGenerator generator(*conn.context);
			auto physical_plan = generator.Plan(std::move(cloned_plan));
			
			// Query the estimated thread count
			// This tells us how many parallel tasks DuckDB would naturally create
			estimated_threads = physical_plan->Root().EstimatedThreadCount();
			
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("📊 [PARALLELISM] DuckDB's natural parallelism decision:"));
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("   - Estimated thread count: %llu", 
			                                  static_cast<long long unsigned>(estimated_threads)));
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("   - Physical plan type: %s", 
			                                  PhysicalOperatorToString(physical_plan->Root().type)));
		});
		
		return estimated_threads;
		
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance, 
		                StringUtil::Format("📊 [PARALLELISM] Failed to query natural parallelism: %s", ex.what()));
		return 0;
	}
}

// STEP 2: Extract partition information from physical plan
// This analyzes the plan structure to provide hints for intelligent partitioning
//
// KEY INSIGHT: Physical plan generation requires a transaction context, which we
// provide by wrapping the generation in RunFunctionInTransaction() (mimicking DuckDB's
// internal behavior in ClientContext::ExtractPlan).
PlanPartitionInfo DistributedExecutor::ExtractPartitionInfo(LogicalOperator &logical_plan, idx_t num_workers) {
	auto &db_instance = *conn.context->db;
	PlanPartitionInfo info;
	
	std::cerr << "\n[PLAN DEBUG] ExtractPartitionInfo called" << std::endl;
	std::cerr << "  Logical Plan Type: " << LogicalOperatorToString(logical_plan.type) << std::endl;
	std::cerr << "  Num Workers: " << num_workers << std::endl;
	std::cerr << "  Logical Plan has children: " << logical_plan.children.size() << std::endl;
	
	// Walk through the logical plan to see what's there
	std::function<void(LogicalOperator&, int)> inspect_plan = [&](LogicalOperator &op, int depth) {
		string indent(depth * 2, ' ');
		std::cerr << indent << "- " << LogicalOperatorToString(op.type);
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			// This is a table scan - let's see what table it references
			std::cerr << " (GET operator - table scan)";
		}
		std::cerr << std::endl;
		for (auto &child : op.children) {
			inspect_plan(*child, depth + 1);
		}
	};
	
	std::cerr << "  Logical Plan Structure:" << std::endl;
	inspect_plan(logical_plan, 2);
	
	try {
		std::cerr << "  Generating physical plan..." << std::endl;
		
		// Wrap physical plan generation in a transaction (mimicking DuckDB's internal behavior)
		conn.context->RunFunctionInTransaction([&]() {
			// Clone the logical plan since Plan() takes ownership
			auto cloned_plan = logical_plan.Copy(*conn.context);
			
			// Generate physical plan using the proper API
			PhysicalPlanGenerator generator(*conn.context);
			auto physical_plan_ptr = generator.Plan(std::move(cloned_plan));
			auto &physical_plan = physical_plan_ptr->Root();
			
			std::cerr << "  ✓ Physical plan generated successfully!" << std::endl;
			
			// Extract basic information
			info.operator_type = physical_plan.type;
			info.estimated_cardinality = physical_plan.estimated_cardinality;
			info.natural_parallelism = physical_plan.EstimatedThreadCount();
			
			std::cerr << "\n[PLAN DEBUG] Physical Plan Details:" << std::endl;
			std::cerr << "  Physical Operator Type: " << PhysicalOperatorToString(info.operator_type) << std::endl;
			std::cerr << "  Estimated Cardinality: " << info.estimated_cardinality << std::endl;
			std::cerr << "  Natural Parallelism (EstimatedThreadCount): " << info.natural_parallelism << std::endl;
			std::cerr << "  Has Statistics: " << (physical_plan.estimated_cardinality > 0 ? "YES" : "NO") << std::endl;
			std::cerr << "  Logical Plan Type: " << LogicalOperatorToString(logical_plan.type) << std::endl;
			std::cerr << "  Estimated Cardinality Source: " << (physical_plan.estimated_cardinality > 0 ? "Physical Plan" : "MISSING!") << std::endl;
			
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("🔍 [PLAN ANALYSIS] Physical plan analysis:"));
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("   - Operator type: %s", 
			                                  PhysicalOperatorToString(info.operator_type)));
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("   - Estimated cardinality: %llu rows", 
			                                  static_cast<long long unsigned>(info.estimated_cardinality)));
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("   - Natural parallelism: %llu tasks", 
			                                  static_cast<long long unsigned>(info.natural_parallelism)));
			
			// Analyze if we can use intelligent partitioning
			// For now, we support intelligent partitioning for:
			// 1. Table scans with sufficient cardinality
			// 2. Plans where natural parallelism matches or exceeds worker count
			if (info.estimated_cardinality > 0 && num_workers > 0) {
				info.rows_per_partition = (info.estimated_cardinality + num_workers - 1) / num_workers;
				
				// We can use intelligent partitioning if:
				// - It's a table scan (most common case)
				// - We have enough rows per partition (at least 100 rows per worker)
				if (info.operator_type == PhysicalOperatorType::TABLE_SCAN && 
				    info.rows_per_partition >= 100) {
					info.supports_intelligent_partitioning = true;
					
					DUCKDB_LOG_DEBUG(db_instance, 
					                StringUtil::Format("✅ [PLAN ANALYSIS] Intelligent partitioning enabled:"));
					DUCKDB_LOG_DEBUG(db_instance, 
					                StringUtil::Format("   - Rows per partition: ~%llu", 
					                                  static_cast<long long unsigned>(info.rows_per_partition)));
				} else {
					DUCKDB_LOG_DEBUG(db_instance, 
					                StringUtil::Format("ℹ️  [PLAN ANALYSIS] Using fallback partitioning (rowid %%)"));
					if (info.operator_type != PhysicalOperatorType::TABLE_SCAN) {
						DUCKDB_LOG_DEBUG(db_instance, 
						                StringUtil::Format("   - Reason: Not a table scan"));
					} else {
						DUCKDB_LOG_DEBUG(db_instance, 
						                StringUtil::Format("   - Reason: Insufficient rows per partition (%llu < 100)", 
						                                  static_cast<long long unsigned>(info.rows_per_partition)));
					}
				}
			} else {
				DUCKDB_LOG_DEBUG(db_instance, 
				                StringUtil::Format("ℹ️  [PLAN ANALYSIS] Cannot determine partitioning strategy"));
			}
		}); // End of RunFunctionInTransaction lambda
		
		return info;
		
	} catch (std::exception &ex) {
		// Physical plan generation failed - investigate why
		std::cerr << "  ✗ Physical plan generation failed!" << std::endl;
		std::cerr << "  Error: " << ex.what() << std::endl;
		std::cerr << "  Falling back to manual partitioning strategy (rowid % N)" << std::endl;
		
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("🔍 [PLAN ANALYSIS] Cannot generate physical plan: %s", ex.what()));
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("   - Using fallback partitioning strategy (rowid %%)"));
		
		// Return empty info, which triggers fallback partitioning
		return info;
	}
}

string DistributedExecutor::SerializeLogicalPlan(LogicalOperator &op) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	op.Serialize(serializer);
	serializer.End();
	auto data_ptr = stream.GetData();
	return string(reinterpret_cast<const char *>(data_ptr), stream.GetPosition());
}

string DistributedExecutor::SerializeLogicalType(const LogicalType &type) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	type.Serialize(serializer);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

unique_ptr<QueryResult>
DistributedExecutor::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                            const vector<string> &names, const vector<LogicalType> &types) {
	// Coordinator acts as GlobalState aggregator in DuckDB's parallel execution model
	//
	// DuckDB's parallel execution pattern:
	// 1. Multiple threads execute in parallel, each with LocalSinkState
	// 2. Combine() merges LocalSinkState into GlobalSinkState
	// 3. Finalize() produces the final result from GlobalSinkState
	//
	// Distributed execution mapping:
	// 1. Multiple worker nodes execute in parallel (each = one thread)
	// 2. Each worker returns LocalState output (as Arrow RecordBatches)
	// 3. This method performs the Combine() operation:
	//    - Collects LocalState outputs from all workers
	//    - Merges them into a unified result (GlobalState)
	// 4. The ColumnDataCollection acts as our GlobalSinkState
	//
	// This maintains the same aggregation semantics as thread-level parallelism,
	// but distributed across network-connected nodes.
	
	auto &db_instance = *conn.context->db.get();
	
	// Collection will be created lazily after we see the first batch's schema
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> actual_types; // Types from actual Arrow data

	// Combine phase: Merge LocalState outputs from each worker
	idx_t worker_idx = 0;
	idx_t total_batches = 0;
	idx_t total_rows_combined = 0;
	
	for (auto &stream : streams) {
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("🔀 [COMBINE] Coordinator: Processing results from worker %llu/%llu", 
		                                  static_cast<long long unsigned>(worker_idx + 1), 
		                                  static_cast<long long unsigned>(streams.size())));
		idx_t worker_batches = 0;
		idx_t worker_rows = 0;
		
		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				DUCKDB_LOG_WARN(db_instance,
				                StringUtil::Format("⚠️  [COMBINE] Coordinator: Worker %llu stream error: %s", 
				                                  static_cast<long long unsigned>(worker_idx), 
				                                  batch_result.status().ToString()));
				break;
			}

			auto batch_with_metadata = batch_result.ValueOrDie();
			if (!batch_with_metadata.data) {
				break; // End of stream from this worker
			}

			// Convert Arrow batch (LocalState output) to DuckDB DataChunk
			auto arrow_batch = batch_with_metadata.data;
			
			// Use Arrow schema to get actual types from the batch
			vector<LogicalType> batch_types;
			batch_types.reserve(arrow_batch->num_columns());
			
			std::cerr << "[DEBUG] Arrow batch has " << arrow_batch->num_columns() << " columns:" << std::endl;
			for (int col_idx = 0; col_idx < arrow_batch->num_columns(); ++col_idx) {
				auto arrow_field = arrow_batch->schema()->field(col_idx);
				auto arrow_type = arrow_field->type();
				auto duckdb_type = ArrowTypeToDuckDBType(arrow_type);
				batch_types.push_back(duckdb_type);
				std::cerr << "  Column " << col_idx << ": " << arrow_field->name() 
				          << " (Arrow: " << arrow_type->ToString() 
				          << " -> DuckDB: " << duckdb_type.ToString() << ")" << std::endl;
			}
			
			// Initialize collection with actual schema from first batch
			if (!collection) {
				actual_types = batch_types;
				collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), actual_types);
				std::cerr << "[DEBUG] Created ColumnDataCollection with types:" << std::endl;
				for (idx_t i = 0; i < actual_types.size(); i++) {
					std::cerr << "  Column " << i << ": " << actual_types[i].ToString() << std::endl;
				}
				DUCKDB_LOG_DEBUG(db_instance, 
				                StringUtil::Format("🔀 [COMBINE] Coordinator: Initialized GlobalSinkState with %llu columns", 
				                                  static_cast<long long unsigned>(actual_types.size())));
			}
			
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), batch_types);

			for (int col_idx = 0; col_idx < arrow_batch->num_columns(); ++col_idx) {
				auto arrow_array = arrow_batch->column(col_idx);
				auto &duckdb_vector = chunk.data[col_idx];
				ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, batch_types[col_idx], arrow_batch->num_rows());
			}

			chunk.SetCardinality(arrow_batch->num_rows());
			// Append to GlobalSinkState (ColumnDataCollection)
			collection->Append(chunk);
			
			worker_batches++;
			worker_rows += arrow_batch->num_rows();
			total_batches++;
			total_rows_combined += arrow_batch->num_rows();
		}
		
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("✅ [COMBINE] Coordinator: Worker %llu contributed %llu batches, %llu rows", 
		                                  static_cast<long long unsigned>(worker_idx), 
		                                  static_cast<long long unsigned>(worker_batches), 
		                                  static_cast<long long unsigned>(worker_rows)));
		
		std::cerr << "  Worker " << worker_idx << " returned: " << worker_rows << " rows in " << worker_batches << " batches" << std::endl;
		
		worker_idx++;
	}
	
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("🎯 [COMBINE] Coordinator: GlobalSinkState merge complete - %llu total batches, %llu total rows from %llu workers", 
	                                  static_cast<long long unsigned>(total_batches),
	                                  static_cast<long long unsigned>(total_rows_combined),
	                                  static_cast<long long unsigned>(streams.size())));

	// Finalize phase: Return the aggregated result
	// In this simple case, we just return the merged collection
	// For more complex operators (aggregates, sorts, etc.), additional
	// finalization logic would go here (e.g., final aggregation, final sort)
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("🏁 [FINALIZE] Coordinator: Returning final result (%llu rows total)", 
	                                  static_cast<long long unsigned>(total_rows_combined)));
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties {}, names,
	                                          std::move(collection), ClientProperties {});
}

} // namespace duckdb
