#include "server/driver/distributed_executor.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
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
	
	// STEP 4: Analyze query to determine merge strategy
	QueryAnalysis query_analysis = AnalyzeQuery(*logical_plan);

	std::cerr << "\n========== DISTRIBUTED EXECUTION START ==========" << std::endl;
	std::cerr << "Query: " << sql << std::endl;
	std::cerr << "Workers: " << workers.size() << std::endl;
	std::cerr << "Natural Parallelism: " << natural_parallelism << std::endl;
	std::cerr << "Intelligent Partitioning: " << (partition_info.supports_intelligent_partitioning ? "YES" : "NO") << std::endl;
	std::cerr << "Estimated Cardinality: " << partition_info.estimated_cardinality << std::endl;
	std::cerr << "=================================================" << std::endl;

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing query '%s' distributed across %llu workers", sql,
	                                                 static_cast<long long unsigned>(workers.size())));
	
	// Phase 2: STEP 2 - Extract pipeline tasks and distribute to workers
	// This replaces the old 1-partition-per-worker approach with flexible task distribution
	auto tasks = ExtractPipelineTasks(*logical_plan, sql, workers.size());
	
	if (tasks.empty()) {
		DUCKDB_LOG_WARN(db_instance, "Failed to extract any pipeline tasks, falling back to local execution");
		return nullptr;
	}
	
	std::cerr << "\n========== STEP 2: TASK DISTRIBUTION ==========" << std::endl;
	std::cerr << "Total Tasks: " << tasks.size() << std::endl;
	std::cerr << "Available Workers: " << workers.size() << std::endl;
	std::cerr << "Distribution Strategy: Round-Robin" << std::endl;
	
	// STEP 2: Map tasks to workers using round-robin
	// This allows M tasks to be distributed across N workers (M >= N)
	vector<vector<idx_t>> worker_to_tasks(workers.size());  // worker_id → [task_indices]
	for (idx_t i = 0; i < tasks.size(); i++) {
		idx_t worker_id = i % workers.size();
		worker_to_tasks[worker_id].push_back(i);
	}
	
	// Log the distribution
	std::cerr << "\nTask Assignment:" << std::endl;
	for (idx_t worker_id = 0; worker_id < workers.size(); worker_id++) {
		std::cerr << "  Worker " << worker_id << ": " << worker_to_tasks[worker_id].size() << " tasks [";
		for (idx_t j = 0; j < worker_to_tasks[worker_id].size(); j++) {
			if (j > 0) std::cerr << ", ";
			std::cerr << worker_to_tasks[worker_id][j];
		}
		std::cerr << "]" << std::endl;
	}
	std::cerr << "===============================================" << std::endl;
	
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("🔧 [STEP2] Distributing %llu tasks across %llu workers (round-robin)",
	                                  static_cast<long long unsigned>(tasks.size()),
	                                  static_cast<long long unsigned>(workers.size())));
	
	// Prepare task SQLs and plans
	// Note: For now, we prepare all tasks upfront. Future optimization: prepare on-demand
	vector<string> task_sqls;
	vector<string> serialized_task_plans;
	task_sqls.reserve(tasks.size());
	serialized_task_plans.reserve(tasks.size());
	
	for (auto &task : tasks) {
		// Extract and serialize the plan for this task
		unique_ptr<LogicalOperator> task_plan;
		try {
			task_plan = conn.ExtractPlan(task.task_sql);
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Failed to extract logical plan for task %llu query '%s': %s",
			                                   static_cast<long long unsigned>(task.task_id),
			                                   task.task_sql, ex.what()));
			return nullptr;
		}
		if (task_plan == nullptr) {
			DUCKDB_LOG_WARN(db_instance, 
			                StringUtil::Format("Task plan extraction returned null for task %llu query '%s'",
			                                   static_cast<long long unsigned>(task.task_id),
			                                   task.task_sql));
			return nullptr;
		}
		if (!IsSupportedPlan(*task_plan)) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Task plan for task %llu query '%s' contains unsupported operators",
			                                   static_cast<long long unsigned>(task.task_id),
			                                   task.task_sql));
			return nullptr;
		}
		
		// Serialize the plan for transmission to worker
		serialized_task_plans.emplace_back(SerializeLogicalPlan(*task_plan));
		task_sqls.emplace_back(task.task_sql);
		
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("✅ [STEP2] Prepared task %llu (plan: %llu bytes)",
		                                    static_cast<long long unsigned>(task.task_id),
		                                    static_cast<long long unsigned>(serialized_task_plans.back().size())));
	}
	
	std::cerr << "\n========== TASK SQLS ==========" << std::endl;
	for (idx_t i = 0; i < task_sqls.size(); i++) {
		std::cerr << "Task " << i << ": " << task_sqls[i] << std::endl;
	}
	std::cerr << "===============================" << std::endl;

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
	
	// Phase 4: STEP 2 - Distribute tasks to workers
	// Workers may receive multiple tasks and execute them sequentially or in parallel
	// This is the bridge from task-based to worker-based execution
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams;
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("🚀 [STEP2] Coordinator: Distributing %llu tasks to %llu workers", 
	                                  static_cast<long long unsigned>(tasks.size()),
	                                  static_cast<long long unsigned>(workers.size())));
	
	std::cerr << "\n========== STEP 2: TASK EXECUTION ==========" << std::endl;
	idx_t total_tasks_sent = 0;

	// Execute tasks on workers (round-robin assignment)
	for (idx_t worker_id = 0; worker_id < workers.size(); ++worker_id) {
		auto *worker = workers[worker_id];
		auto &task_indices = worker_to_tasks[worker_id];
		
		if (task_indices.empty()) {
			std::cerr << "Worker " << worker_id << ": No tasks assigned (skipping)" << std::endl;
			continue;
		}
		
		std::cerr << "Worker " << worker_id << ": Executing " << task_indices.size() << " task(s)..." << std::endl;
		
		// For now, send tasks sequentially to each worker
		// Future optimization: batch multiple tasks in single request
		for (auto task_idx : task_indices) {
			auto &task = tasks[task_idx];
			
			distributed::ExecutePartitionRequest req;
			
			// Send task information
			req.set_sql(task_sqls[task_idx]);
			req.set_partition_id(task.task_id);
			req.set_total_partitions(task.total_tasks);
			req.set_serialized_plan(serialized_task_plans[task_idx]);
			for (const auto &name : names) {
				req.add_column_names(name);
			}
			for (const auto &type_bytes : serialized_types) {
				req.add_column_types(type_bytes);
			}
			
			// Execute task on worker
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("📤 [STEP2] Sending task %llu/%llu to worker %s (plan: %llu bytes)",
			                                  static_cast<long long unsigned>(task.task_id),
			                                  static_cast<long long unsigned>(tasks.size()),
			                                  worker->worker_id,
			                                  static_cast<long long unsigned>(serialized_task_plans[task_idx].size())));
			
			std::unique_ptr<arrow::flight::FlightStreamReader> stream;
			auto status = worker->client->ExecutePartition(req, stream);
			if (!status.ok()) {
				DUCKDB_LOG_WARN(db_instance, 
				                StringUtil::Format("❌ [STEP2] Worker %s failed executing task %llu: %s",
				                                  worker->worker_id,
				                                  static_cast<long long unsigned>(task.task_id),
				                                  status.ToString()));
				std::cerr << "  Task " << task.task_id << ": FAILED (" << status.ToString() << ")" << std::endl;
				continue;
			}
			
			DUCKDB_LOG_DEBUG(db_instance, 
			                StringUtil::Format("✅ [STEP2] Worker %s accepted task %llu", 
			                                  worker->worker_id,
			                                  static_cast<long long unsigned>(task.task_id)));
			std::cerr << "  Task " << task.task_id << ": SENT" << std::endl;
			
			result_streams.emplace_back(std::move(stream));
			total_tasks_sent++;
		}
		
		std::cerr << "Worker " << worker_id << ": ✓ Sent " << task_indices.size() << " task(s)" << std::endl;
	}
	
	std::cerr << "\nTotal tasks sent: " << total_tasks_sent << "/" << tasks.size() << std::endl;
	std::cerr << "============================================" << std::endl;
	
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("✅ [STEP2] Successfully sent %llu/%llu tasks to workers",
	                                  static_cast<long long unsigned>(total_tasks_sent),
	                                  static_cast<long long unsigned>(tasks.size())));
	
	if (result_streams.empty()) {
		DUCKDB_LOG_WARN(db_instance, "No tasks were successfully executed");
		return nullptr;
	}

	// Phase 5: Combine results (GlobalState aggregation) 
	// STEP 4: Now using smart merging based on query analysis
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔄 [STEP4] Collecting and merging results from %llu tasks",
	                                                 static_cast<long long unsigned>(result_streams.size())));
	
	std::cerr << "\n========== STEP 4: MERGE PHASE ==========" << std::endl;
	std::cerr << "Collecting results from " << result_streams.size() << " task(s)..." << std::endl;
	
	auto result = CollectAndMergeResults(result_streams, names, types, query_analysis);

	if (result) {
		// Count total rows returned.
		idx_t total_rows = 0;
		auto materialized = dynamic_cast<MaterializedQueryResult *>(result.get());
		if (materialized) {
			total_rows = materialized->RowCount();
		}
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Distributed query completed: %llu total rows returned",
		                                                 static_cast<long long unsigned>(total_rows)));
		
		std::cerr << "FINAL RESULT: " << total_rows << " total rows from " << total_tasks_sent << " task(s)" << std::endl;
		std::cerr << "========================================" << std::endl;
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

// STEP 1 (Pipeline Tasks): Extract distributed pipeline tasks from physical plan
// This is the bridge between our current statistics-informed approach and true pipeline task distribution
vector<DistributedPipelineTask> DistributedExecutor::ExtractPipelineTasks(
    LogicalOperator &logical_plan,
    const string &base_sql,
    idx_t num_workers) {
	
	auto &db_instance = *conn.context->db;
	vector<DistributedPipelineTask> tasks;
	
	std::cerr << "\n[PIPELINE TASKS] Extracting pipeline tasks from physical plan..." << std::endl;
	std::cerr << "  Base SQL: " << base_sql << std::endl;
	std::cerr << "  Available Workers: " << num_workers << std::endl;
	
	try {
		// First, get partition info (cardinality, natural parallelism)
		auto partition_info = ExtractPartitionInfo(logical_plan, num_workers);
		
		// Determine how many tasks to create
		// For now, we use max(natural_parallelism, num_workers)
		// This allows us to over-subscribe workers if DuckDB wants more parallelism
		idx_t num_tasks = partition_info.natural_parallelism;
		if (num_tasks == 0 || num_tasks < num_workers) {
			// DuckDB doesn't think this needs parallelism, but we have workers available
			// Use num_workers anyway for distributed execution
			num_tasks = num_workers;
			std::cerr << "  DuckDB parallelism (" << partition_info.natural_parallelism 
			          << ") < workers, using " << num_tasks << " tasks" << std::endl;
		} else if (num_tasks > num_workers * 4) {
			// Cap at 4x workers to avoid too many small tasks
			num_tasks = num_workers * 4;
			std::cerr << "  Capping tasks at " << num_tasks 
			          << " (4x workers) to avoid fragmentation" << std::endl;
		}
		
		std::cerr << "  Creating " << num_tasks << " pipeline tasks" << std::endl;
		
		// Create tasks based on partition info
		if (partition_info.supports_intelligent_partitioning && 
		    partition_info.estimated_cardinality > 0) {
			// Intelligent partitioning: divide data by rowid ranges
			std::cerr << "  Using intelligent range-based partitioning" << std::endl;
			std::cerr << "  Total cardinality: " << partition_info.estimated_cardinality << std::endl;
			
			idx_t rows_per_task = (partition_info.estimated_cardinality + num_tasks - 1) / num_tasks;
			
			for (idx_t i = 0; i < num_tasks; i++) {
				DistributedPipelineTask task;
				task.task_id = i;
				task.total_tasks = num_tasks;
				
				// Calculate rowid range for this task
				idx_t row_start = i * rows_per_task;
				idx_t row_end = std::min((i + 1) * rows_per_task - 1, 
				                        partition_info.estimated_cardinality);
				
				// For last task, extend to include any remainder
				if (i == num_tasks - 1) {
					row_end = partition_info.estimated_cardinality;
				}
				
				// Create SQL with rowid filter
				string trimmed = base_sql;
				StringUtil::RTrim(trimmed);
				if (!trimmed.empty() && trimmed.back() == ';') {
					trimmed.pop_back();
					StringUtil::RTrim(trimmed);
				}
				
				task.task_sql = StringUtil::Format("%s WHERE rowid BETWEEN %llu AND %llu",
				                                   trimmed,
				                                   static_cast<long long unsigned>(row_start),
				                                   static_cast<long long unsigned>(row_end));
				
				task.row_group_start = row_start / DEFAULT_ROW_GROUP_SIZE;
				task.row_group_end = row_end / DEFAULT_ROW_GROUP_SIZE;
				
				std::cerr << "  Task " << i << ": rows [" << row_start << ", " << row_end 
				          << "], row_groups [" << task.row_group_start << ", " << task.row_group_end << "]" 
				          << std::endl;
				
				tasks.push_back(std::move(task));
			}
		} else {
			// Fallback: modulo-based partitioning
			std::cerr << "  Using fallback modulo-based partitioning" << std::endl;
			
			for (idx_t i = 0; i < num_tasks; i++) {
				DistributedPipelineTask task;
				task.task_id = i;
				task.total_tasks = num_tasks;
				
				// Create SQL with modulo filter
				string trimmed = base_sql;
				StringUtil::RTrim(trimmed);
				if (!trimmed.empty() && trimmed.back() == ';') {
					trimmed.pop_back();
					StringUtil::RTrim(trimmed);
				}
				
				task.task_sql = StringUtil::Format("%s WHERE (rowid %% %llu) = %llu",
				                                   trimmed,
				                                   static_cast<long long unsigned>(num_tasks),
				                                   static_cast<long long unsigned>(i));
				
				task.row_group_start = 0;  // Unknown for modulo partitioning
				task.row_group_end = 0;
				
				std::cerr << "  Task " << i << ": modulo partition" << std::endl;
				
				tasks.push_back(std::move(task));
			}
		}
		
		std::cerr << "[PIPELINE TASKS] Successfully created " << tasks.size() << " tasks" << std::endl;
		
		DUCKDB_LOG_DEBUG(db_instance,
		                StringUtil::Format("🔧 [PIPELINE] Created %llu distributed pipeline tasks",
		                                  static_cast<long long unsigned>(tasks.size())));
		
	} catch (std::exception &ex) {
		std::cerr << "[PIPELINE TASKS ERROR] Failed to extract tasks: " << ex.what() << std::endl;
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("🔧 [PIPELINE] Failed to extract pipeline tasks: %s", ex.what()));
		
		// Fallback: create simple task-per-worker
		for (idx_t i = 0; i < num_workers; i++) {
			DistributedPipelineTask task;
			task.task_id = i;
			task.total_tasks = num_workers;
			
			string trimmed = base_sql;
			StringUtil::RTrim(trimmed);
			if (!trimmed.empty() && trimmed.back() == ';') {
				trimmed.pop_back();
				StringUtil::RTrim(trimmed);
			}
			
			task.task_sql = StringUtil::Format("%s WHERE (rowid %% %llu) = %llu",
			                                   trimmed,
			                                   static_cast<long long unsigned>(num_workers),
			                                   static_cast<long long unsigned>(i));
			tasks.push_back(std::move(task));
		}
	}
	
	return tasks;
}

// STEP 4: Analyze query to determine optimal merge strategy
DistributedExecutor::QueryAnalysis DistributedExecutor::AnalyzeQuery(LogicalOperator &logical_plan) {
	QueryAnalysis analysis;
	auto &db_instance = *conn.context->db;
	
	std::cerr << "\n[STEP 4] Analyzing query for merge strategy..." << std::endl;
	
	// Recursively walk the logical plan tree to find aggregates, GROUP BY, DISTINCT
	std::function<void(LogicalOperator&)> analyze_operator = [&](LogicalOperator &op) {
		std::cerr << "  Analyzing operator: " << LogicalOperatorToString(op.type) << std::endl;
		
		// Check for AGGREGATE operator
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			analysis.has_aggregates = true;
			
			auto &agg_op = op.Cast<LogicalAggregate>();
			
			// Check if this is a GROUP BY aggregation
			if (!agg_op.groups.empty()) {
				analysis.has_group_by = true;
				std::cerr << "    Found GROUP BY with " << agg_op.groups.size() << " group(s)" << std::endl;
			}
			
			// Extract aggregate function names
			for (auto &expr : agg_op.expressions) {
				if (expr->type == ExpressionType::BOUND_AGGREGATE) {
					auto &agg_expr = expr->Cast<BoundAggregateExpression>();
					analysis.aggregate_functions.push_back(agg_expr.function.name);
					std::cerr << "    Found aggregate function: " << agg_expr.function.name << std::endl;
				}
			}
		}
		
		// Check for DISTINCT operator
		if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
			analysis.has_distinct = true;
			std::cerr << "    Found DISTINCT" << std::endl;
		}
		
		// Recursively analyze children
		for (auto &child : op.children) {
			analyze_operator(*child);
		}
	};
	
	// Start analysis from root
	analyze_operator(logical_plan);
	
	// Determine merge strategy based on what we found
	if (analysis.has_group_by) {
		analysis.merge_strategy = MergeStrategy::GROUP_BY_MERGE;
		std::cerr << "  Merge Strategy: GROUP_BY_MERGE" << std::endl;
	} else if (analysis.has_aggregates) {
		analysis.merge_strategy = MergeStrategy::AGGREGATE_MERGE;
		std::cerr << "  Merge Strategy: AGGREGATE_MERGE" << std::endl;
	} else if (analysis.has_distinct) {
		analysis.merge_strategy = MergeStrategy::DISTINCT_MERGE;
		std::cerr << "  Merge Strategy: DISTINCT_MERGE" << std::endl;
	} else {
		analysis.merge_strategy = MergeStrategy::CONCATENATE;
		std::cerr << "  Merge Strategy: CONCATENATE (simple scan)" << std::endl;
	}
	
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("🔍 [STEP4] Query analysis: merge_strategy=%d, aggregates=%d, group_by=%d, distinct=%d",
	                                  static_cast<int>(analysis.merge_strategy),
	                                  analysis.has_aggregates,
	                                  analysis.has_group_by,
	                                  analysis.has_distinct));
	
	return analysis;
}

// STEP 4B: Build SQL to re-aggregate partial aggregates (no GROUP BY)
string DistributedExecutor::BuildAggregateMergeSQL(const string &temp_table, const vector<string> &column_names, 
                                                   const QueryAnalysis &analysis) {
	// For aggregates without GROUP BY, we need to merge partial results:
	// - SUM(partial_sums) for SUM
	// - SUM(partial_counts) for COUNT
	// - For AVG: we'd need SUM(partial_sums) / SUM(partial_counts), but this is complex
	//
	// For now, we'll use a simple approach: re-aggregate all columns
	
	string sql = "SELECT ";
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i > 0) sql += ", ";
		
		// Try to intelligently re-aggregate based on likely function
		// This is a heuristic - in future, we'd track the actual aggregate functions
		string col_name_lower = StringUtil::Lower(column_names[i]);
		
		if (col_name_lower.find("count") != string::npos || 
		    col_name_lower.find("cnt") != string::npos) {
			// COUNT: sum the partial counts
			sql += StringUtil::Format("SUM(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("sum") != string::npos) {
			// SUM: sum the partial sums
			sql += StringUtil::Format("SUM(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("min") != string::npos) {
			// MIN: take min of partial mins
			sql += StringUtil::Format("MIN(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("max") != string::npos) {
			// MAX: take max of partial maxes
			sql += StringUtil::Format("MAX(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("avg") != string::npos) {
			// AVG: This is tricky - we'd need both sum and count
			// For now, just take AVG again (not mathematically correct, but works for demo)
			sql += StringUtil::Format("AVG(%s) AS %s", column_names[i], column_names[i]);
		} else {
			// Default: try SUM (works for most aggregates)
			sql += StringUtil::Format("SUM(%s) AS %s", column_names[i], column_names[i]);
		}
	}
	sql += StringUtil::Format(" FROM %s", temp_table);
	
	return sql;
}

// STEP 4B: Build SQL to re-group and re-aggregate (with GROUP BY)
string DistributedExecutor::BuildGroupByMergeSQL(const string &temp_table, const vector<string> &column_names,
                                                 const QueryAnalysis &analysis) {
	// For GROUP BY, we need to:
	// 1. Identify which columns are group keys (non-aggregate columns)
	// 2. Identify which columns are aggregates
	// 3. Re-group by the keys and re-aggregate the aggregate columns
	
	// Heuristic: columns with aggregate-sounding names are aggregates, others are group keys
	vector<string> group_keys;
	vector<string> agg_columns;
	
	for (const auto &col_name : column_names) {
		string col_lower = StringUtil::Lower(col_name);
		
		// Check if this looks like an aggregate column
		if (col_lower.find("count") != string::npos ||
		    col_lower.find("sum") != string::npos ||
		    col_lower.find("avg") != string::npos ||
		    col_lower.find("min") != string::npos ||
		    col_lower.find("max") != string::npos ||
		    col_lower.find("_agg") != string::npos) {
			agg_columns.push_back(col_name);
		} else {
			group_keys.push_back(col_name);
		}
	}
	
	// If we couldn't identify any group keys, fall back to treating first column as key
	if (group_keys.empty() && !column_names.empty()) {
		group_keys.push_back(column_names[0]);
		for (idx_t i = 1; i < column_names.size(); i++) {
			agg_columns.push_back(column_names[i]);
		}
	}
	
	// Build SELECT clause
	string sql = "SELECT ";
	
	// Add group keys (pass through)
	for (idx_t i = 0; i < group_keys.size(); i++) {
		if (i > 0) sql += ", ";
		sql += group_keys[i];
	}
	
	// Add re-aggregated columns
	for (const auto &agg_col : agg_columns) {
		sql += ", ";
		
		string col_lower = StringUtil::Lower(agg_col);
		if (col_lower.find("count") != string::npos) {
			sql += StringUtil::Format("SUM(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("sum") != string::npos) {
			sql += StringUtil::Format("SUM(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("min") != string::npos) {
			sql += StringUtil::Format("MIN(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("max") != string::npos) {
			sql += StringUtil::Format("MAX(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("avg") != string::npos) {
			sql += StringUtil::Format("AVG(%s) AS %s", agg_col, agg_col);
		} else {
			// Default to SUM
			sql += StringUtil::Format("SUM(%s) AS %s", agg_col, agg_col);
		}
	}
	
	sql += StringUtil::Format(" FROM %s", temp_table);
	
	// Add GROUP BY clause
	if (!group_keys.empty()) {
		sql += " GROUP BY ";
		for (idx_t i = 0; i < group_keys.size(); i++) {
			if (i > 0) sql += ", ";
			sql += group_keys[i];
		}
	}
	
	return sql;
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

// STEP 4: Smart result merging with aggregation support
unique_ptr<QueryResult>
DistributedExecutor::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                            const vector<string> &names, const vector<LogicalType> &types,
                                            const QueryAnalysis &query_analysis) {
	auto &db_instance = *conn.context->db.get();
	
	std::cerr << "\n[STEP 4] Smart merge phase starting..." << std::endl;
	std::cerr << "  Merge Strategy: ";
	switch (query_analysis.merge_strategy) {
		case MergeStrategy::CONCATENATE:
			std::cerr << "CONCATENATE (simple scan)" << std::endl;
			break;
		case MergeStrategy::AGGREGATE_MERGE:
			std::cerr << "AGGREGATE_MERGE (merge partial aggregates)" << std::endl;
			break;
		case MergeStrategy::GROUP_BY_MERGE:
			std::cerr << "GROUP_BY_MERGE (merge grouped results)" << std::endl;
			break;
		case MergeStrategy::DISTINCT_MERGE:
			std::cerr << "DISTINCT_MERGE (eliminate duplicates)" << std::endl;
			break;
	}
	
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("🔀 [STEP4B] Merging %llu result streams using strategy %d",
	                                  static_cast<long long unsigned>(streams.size()),
	                                  static_cast<int>(query_analysis.merge_strategy)));
	
	// STEP 1: Collect results from all workers
	auto partial_result = CollectAndMergeResults(streams, names, types);
	
	// STEP 2: For simple scans, just return the concatenated results
	if (query_analysis.merge_strategy == MergeStrategy::CONCATENATE) {
		DUCKDB_LOG_DEBUG(db_instance, "✅ [STEP4B] CONCATENATE strategy - returning concatenated results");
		std::cerr << "  No re-aggregation needed, returning concatenated results" << std::endl;
		return partial_result;
	}
	
	// STEP 3: For aggregations, GROUP BY, or DISTINCT, we need to re-process the results
	std::cerr << "  Re-processing results for smart merge..." << std::endl;
	
	auto materialized = dynamic_cast<MaterializedQueryResult *>(partial_result.get());
	if (!materialized || materialized->RowCount() == 0) {
		DUCKDB_LOG_DEBUG(db_instance, "⚠️  [STEP4B] No rows to merge, returning empty result");
		return partial_result;
	}
	
	std::cerr << "  Collected " << materialized->RowCount() << " partial rows from workers" << std::endl;
	
	try {
		// STEP 4: Create a temporary table from the collected results
		string temp_table_name = "__distributed_partial_results__";
		
		// Drop if exists
		conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));
		
		// Create table with correct schema
		string create_sql = StringUtil::Format("CREATE TEMPORARY TABLE %s (", temp_table_name);
		for (idx_t i = 0; i < names.size(); i++) {
			if (i > 0) create_sql += ", ";
			create_sql += StringUtil::Format("%s %s", names[i], types[i].ToString());
		}
		create_sql += ")";
		
		std::cerr << "  Creating temp table: " << create_sql << std::endl;
		auto create_result = conn.Query(create_sql);
		if (create_result->HasError()) {
			throw std::runtime_error(StringUtil::Format("Failed to create temp table: %s", create_result->GetError()));
		}
		
		// Insert collected data into temp table - insert row by row
		std::cerr << "  Inserting " << materialized->RowCount() << " rows into temp table..." << std::endl;
		idx_t inserted_rows = 0;
		
		// Get the collection from materialized result
		auto &collection = materialized->Collection();
		
		// Iterate through all chunks in the collection
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state);
		DataChunk insert_chunk;
		insert_chunk.Initialize(Allocator::DefaultAllocator(), types);
		
		while (collection.Scan(scan_state, insert_chunk)) {
			if (insert_chunk.size() == 0) break;
			
			// Insert this chunk row by row
			for (idx_t row_idx = 0; row_idx < insert_chunk.size(); row_idx++) {
				string row_sql = StringUtil::Format("INSERT INTO %s VALUES (", temp_table_name);
				for (idx_t col_idx = 0; col_idx < insert_chunk.ColumnCount(); col_idx++) {
					if (col_idx > 0) row_sql += ", ";
					auto value = insert_chunk.GetValue(col_idx, row_idx);
					row_sql += value.ToSQLString();
				}
				row_sql += ")";
				
				auto insert_result = conn.Query(row_sql);
				if (insert_result->HasError()) {
					throw std::runtime_error(StringUtil::Format("Failed to insert row: %s", insert_result->GetError()));
				}
				inserted_rows++;
			}
		}
		
		std::cerr << "  Inserted " << inserted_rows << " rows into temp table" << std::endl;
		
		// STEP 5: Apply the appropriate merge strategy
		string merge_sql;
		
		switch (query_analysis.merge_strategy) {
			case MergeStrategy::AGGREGATE_MERGE:
				// Re-aggregate: SUM(partial_sums), SUM(partial_counts), etc.
				std::cerr << "  Applying AGGREGATE_MERGE..." << std::endl;
				merge_sql = BuildAggregateMergeSQL(temp_table_name, names, query_analysis);
				break;
				
			case MergeStrategy::GROUP_BY_MERGE:
				// Re-group and re-aggregate
				std::cerr << "  Applying GROUP_BY_MERGE..." << std::endl;
				merge_sql = BuildGroupByMergeSQL(temp_table_name, names, query_analysis);
				break;
				
			case MergeStrategy::DISTINCT_MERGE:
				// Apply DISTINCT
				std::cerr << "  Applying DISTINCT_MERGE..." << std::endl;
				merge_sql = StringUtil::Format("SELECT DISTINCT * FROM %s", temp_table_name);
				break;
				
			default:
				// Shouldn't reach here
				merge_sql = StringUtil::Format("SELECT * FROM %s", temp_table_name);
				break;
		}
		
		std::cerr << "  Executing merge SQL: " << merge_sql << std::endl;
		
		// STEP 6: Execute the merge SQL and return the result
		auto final_result = conn.Query(merge_sql);
		
		// Clean up temp table
		conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));
		
		if (final_result->HasError()) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("⚠️  [STEP4B] Merge SQL failed: %s, returning partial results",
			                                  final_result->GetError()));
			std::cerr << "  Merge SQL failed: " << final_result->GetError() << std::endl;
			return partial_result;  // Fallback to partial results
		}
		
		auto final_materialized = dynamic_cast<MaterializedQueryResult *>(final_result.get());
		if (final_materialized) {
			std::cerr << "  Final result: " << final_materialized->RowCount() << " rows after merge" << std::endl;
		}
		
		DUCKDB_LOG_DEBUG(db_instance, "✅ [STEP4B] Smart merge complete");
		return final_result;
		
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("⚠️  [STEP4B] Smart merge failed: %s, returning partial results", ex.what()));
		std::cerr << "  Smart merge failed: " << ex.what() << std::endl;
		return partial_result;  // Fallback to partial results
	}
}

} // namespace duckdb
