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
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "server/driver/worker_manager.hpp"

#include <functional>

namespace duckdb {

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p)
    : worker_manager(worker_manager_p), conn(conn_p) {
	// Initialize extracted modules
	plan_analyzer = make_uniq<QueryPlanAnalyzer>(conn);
	sql_generator = make_uniq<PartitionSQLGenerator>();
	result_merger = make_uniq<ResultMerger>(conn);
	task_partitioner = make_uniq<TaskPartitioner>(conn, *plan_analyzer, *sql_generator);
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

	auto &db_instance = *conn.context->db;

	if (!CanDistribute(sql)) {
		return nullptr;
	}

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
	if (!plan_analyzer->IsSupportedPlan(*logical_plan)) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Logical plan for query '%s' contains unsupported operators", sql));
		return nullptr;
	}

	// STEP 1: Query what DuckDB would naturally do for parallelism
	// This helps us understand DuckDB's intelligent parallelization decisions
	idx_t estimated_parallelism = plan_analyzer->QueryNaturalParallelism(*logical_plan);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("📊 [STEP1] DuckDB would naturally use %llu parallel tasks",
	                                                 static_cast<long long unsigned>(estimated_parallelism)));
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("📊 [STEP1] We have %llu workers available",
	                                                 static_cast<long long unsigned>(workers.size())));
	if (estimated_parallelism > 0 && estimated_parallelism != workers.size()) {
		DUCKDB_LOG_DEBUG(
		    db_instance,
		    StringUtil::Format("📊 [STEP1] NOTE: Mismatch between natural parallelism (%llu) and worker count (%llu)",
		                       static_cast<long long unsigned>(estimated_parallelism),
		                       static_cast<long long unsigned>(workers.size())));
	}

	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("[DIST] ExecuteDistributed: '%s' workers=%llu estimated_parallelism=%llu", sql,
	                                    static_cast<long long unsigned>(workers.size()),
	                                    static_cast<long long unsigned>(estimated_parallelism)));

	// STEP 2: Extract partition information from physical plan
	// This analyzes the plan to determine if we can use intelligent partitioning
	PlanPartitionInfo partition_info = plan_analyzer->ExtractPartitionInfo(*logical_plan, workers.size());
	DUCKDB_LOG_DEBUG(
	    db_instance,
	    StringUtil::Format("📊 [STEP2] Plan analysis complete - intelligent partitioning: %s",
	                       partition_info.supports_intelligent_partitioning ? "YES" : "NO (using rowid %%)"));

	// STEP 4: Analyze query to determine merge strategy
	QueryPlanAnalyzer::QueryAnalysis query_analysis = plan_analyzer->AnalyzeQuery(*logical_plan);

	// STEP 6: Analyze pipeline complexity
	QueryPlanAnalyzer::PipelineInfo pipeline_info = plan_analyzer->AnalyzePipelines(*logical_plan);

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing query '%s' distributed across %llu workers", sql,
	                                                 static_cast<long long unsigned>(workers.size())));

	// Phase 2: STEP 2 - Extract pipeline tasks and distribute to workers
	// This replaces the old 1-partition-per-worker approach with flexible task distribution
	auto tasks = task_partitioner->ExtractPipelineTasks(*logical_plan, sql, workers.size());

	if (tasks.empty()) {
		DUCKDB_LOG_WARN(db_instance, "Failed to extract any pipeline tasks, falling back to local execution");
		return nullptr;
	}

	// STEP 2: Map tasks to workers using round-robin
	// This allows M tasks to be distributed across N workers (M >= N)
	vector<vector<idx_t>> worker_to_tasks(workers.size()); // worker_id → [task_indices]
	for (idx_t i = 0; i < tasks.size(); i++) {
		idx_t worker_id = i % workers.size();
		worker_to_tasks[worker_id].push_back(i);
	}

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
			DUCKDB_LOG_WARN(db_instance, StringUtil::Format(
			                                 "Failed to extract logical plan for task %llu query '%s': %s",
			                                 static_cast<long long unsigned>(task.task_id), task.task_sql, ex.what()));
			return nullptr;
		}
		if (task_plan == nullptr) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Task plan extraction returned null for task %llu query '%s'",
			                                   static_cast<long long unsigned>(task.task_id), task.task_sql));
			return nullptr;
		}
		if (!plan_analyzer->IsSupportedPlan(*task_plan)) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Task plan for task %llu query '%s' contains unsupported operators",
			                                   static_cast<long long unsigned>(task.task_id), task.task_sql));
			return nullptr;
		}

		// Serialize the plan for transmission to worker
		serialized_task_plans.emplace_back(PlanSerializer::SerializeLogicalPlan(*task_plan));
		task_sqls.emplace_back(task.task_sql);

		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("✅ [STEP2] Prepared task %llu (plan: %llu bytes)",
		                                    static_cast<long long unsigned>(task.task_id),
		                                    static_cast<long long unsigned>(serialized_task_plans.back().size())));
	}

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
		serialized_types.emplace_back(PlanSerializer::SerializeLogicalType(type));
	}

	// Phase 4: STEP 2 - Distribute tasks to workers
	// Workers may receive multiple tasks and execute them sequentially or in parallel
	// This is the bridge from task-based to worker-based execution
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🚀 [STEP2] Coordinator: Distributing %llu tasks to %llu workers",
	                                                 static_cast<long long unsigned>(tasks.size()),
	                                                 static_cast<long long unsigned>(workers.size())));

	idx_t total_tasks_sent = 0;

	// Execute tasks on workers (round-robin assignment)
	for (idx_t worker_id = 0; worker_id < workers.size(); ++worker_id) {
		auto *worker = workers[worker_id];
		auto &task_indices = worker_to_tasks[worker_id];

		if (task_indices.empty()) {
			continue;
		}

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
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format(
			                                  "📤 [STEP2] Sending task %llu/%llu to worker %s (plan: %llu bytes)",
			                                  static_cast<long long unsigned>(task.task_id),
			                                  static_cast<long long unsigned>(tasks.size()), worker->worker_id,
			                                  static_cast<long long unsigned>(serialized_task_plans[task_idx].size())));

			std::unique_ptr<arrow::flight::FlightStreamReader> stream;
			auto status = worker->client->ExecutePartition(req, stream);
			if (!status.ok()) {
				DUCKDB_LOG_WARN(db_instance,
				                StringUtil::Format("❌ [STEP2] Worker %s failed executing task %llu: %s",
				                                   worker->worker_id, static_cast<long long unsigned>(task.task_id),
				                                   status.ToString()));
				continue;
			}

			DUCKDB_LOG_DEBUG(db_instance,
			                 StringUtil::Format("✅ [STEP2] Worker %s accepted task %llu", worker->worker_id,
			                                    static_cast<long long unsigned>(task.task_id)));

			result_streams.emplace_back(std::move(stream));
			total_tasks_sent += task_indices.size();
		}
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("✅ [STEP2] Successfully sent %llu/%llu tasks to workers",
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

	auto result = result_merger->CollectAndMergeResults(result_streams, names, types, query_analysis);

	idx_t total_rows = 0;
	if (result) {
		auto materialized = dynamic_cast<MaterializedQueryResult *>(result.get());
		if (materialized) {
			total_rows = materialized->RowCount();
		}
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Distributed query completed: %llu total rows returned",
		                                                 static_cast<long long unsigned>(total_rows)));
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("[DIST] Merge complete: rows=%llu tasks=%llu",
	                                                 static_cast<long long unsigned>(total_rows),
	                                                 static_cast<long long unsigned>(total_tasks_sent)));

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

	return true;
}

} // namespace duckdb
