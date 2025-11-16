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

namespace duckdb {

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p)
    : worker_manager(worker_manager_p), conn(conn_p) {
	plan_analyzer = make_uniq<QueryPlanAnalyzer>(conn);
	sql_generator = make_uniq<PartitionSQLGenerator>();
	result_merger = make_uniq<ResultMerger>(conn);
	task_partitioner = make_uniq<TaskPartitioner>(conn, *plan_analyzer, *sql_generator);
}

// Distributed execution Driver implementing DuckDB's parallel execution model.
//
// Architecture mapping (thread-based -> node-based):
//
// DuckDB Parallel Execution:
// 1. Query is compiled to a physical plan
// 2. Data is partitioned across multiple threads
// 3. Each thread executes with LocalSinkState
// 4. Results are combined into GlobalSinkState
// 5. Final result is produced
//
// Distributed Execution:
// 1. Query is compiled to a logical/physical plan [Driver]
// 2. Plan is partitioned and sent to worker nodes [Driver]
// 3. Each worker executes its partition (LocalState semantics) [WORKER]
// 4. Driver collects and combines results (GlobalState semantics) [Driver]
// 5. Final result is returned to client [Driver]
DistributedExecutionResult DistributedExecutor::ExecuteDistributed(const string &sql) {
	DistributedExecutionResult exec_result;
	auto &db_instance = *conn.context->db;

	// Start timing worker execution
	auto worker_start = std::chrono::high_resolution_clock::now();

	if (!CanDistribute(sql)) {
		return exec_result;  // Returns with result = nullptr
	}

	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return exec_result;  // Returns with result = nullptr
	}

	exec_result.num_workers_used = workers.size();

	// Phase 1: Plan extraction and validation
	unique_ptr<LogicalOperator> logical_plan = conn.ExtractPlan(sql);
	if (logical_plan == nullptr) {
		return exec_result;
	}
	if (!plan_analyzer->IsSupportedPlan(*logical_plan)) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Logical plan for query '%s' contains unsupported operators", sql));
		return exec_result;
	}

	// Analyze query to determine merge strategy
	QueryPlanAnalyzer::QueryAnalysis query_analysis = plan_analyzer->AnalyzeQuery(*logical_plan);
	exec_result.merge_strategy = query_analysis.merge_strategy;

	// Analyze pipeline complexity
	QueryPlanAnalyzer::PipelineInfo pipeline_info = plan_analyzer->AnalyzePipelines(*logical_plan);

	// Phase 2: Extract pipeline tasks and distribute to workers
	// This replaces the old 1-partition-per-worker approach with flexible task distribution
	auto tasks = task_partitioner->ExtractPipelineTasks(*logical_plan, sql, workers.size());
	if (tasks.empty()) {
		return exec_result;
	}

	// Map tasks to workers using round-robin
	// This allows M tasks to be distributed across N workers (M >= N)
	// Maps from worker_id -> [task_indices]
	vector<vector<idx_t>> worker_to_tasks(workers.size());
	for (idx_t idx = 0; idx < tasks.size(); ++idx) {
		const idx_t worker_id = idx % workers.size();
		worker_to_tasks[worker_id].emplace_back(idx);
	}

	// Prepare task SQLs and plans
	// Note: For now, we prepare all tasks upfront. Future optimization: prepare on-demand
	vector<string> task_sqls;
	vector<string> serialized_task_plans;
	task_sqls.reserve(tasks.size());
	serialized_task_plans.reserve(tasks.size());

	for (auto &task : tasks) {
		// Extract and serialize the plan for this task
		auto task_plan = conn.ExtractPlan(task.task_sql);
		if (task_plan == nullptr) {
			return exec_result;
		}
		if (!plan_analyzer->IsSupportedPlan(*task_plan)) {
			return exec_result;
		}

		// Serialize the plan for transmission to worker.
		serialized_task_plans.emplace_back(PlanSerializer::SerializeLogicalPlan(*task_plan));
		task_sqls.emplace_back(task.task_sql);
	}

	// Phase 3: Prepare result schema and type information。
	auto prepared = conn.Prepare(sql);
	if (prepared->HasError()) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Failed to prepare distributed query '%s': %s", sql, prepared->GetError()));
		return exec_result;
	}

	vector<string> names = prepared->GetNames();
	vector<LogicalType> types = prepared->GetTypes();
	vector<string> serialized_types;
	serialized_types.reserve(types.size());
	for (auto &type : types) {
		serialized_types.emplace_back(PlanSerializer::SerializeLogicalType(type));
	}

	// Phase 4: Distribute tasks to workers。
	// Workers may receive multiple tasks and execute them sequentially or in parallel。
	// This is the bridge from task-based to worker-based execution。
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams;

	// Execute tasks on workers with round-robin assignment.
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

			// Send task information.
			distributed::ExecutePartitionRequest req;
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

			// Execute task on worker.
			std::unique_ptr<arrow::flight::FlightStreamReader> stream;
			auto status = worker->client->ExecutePartition(req, stream);
			if (!status.ok()) {
				DUCKDB_LOG_WARN(db_instance,
				                StringUtil::Format("Worker %s failed executing task %llu: %s", worker->worker_id,
				                                   static_cast<long long unsigned>(task.task_id), status.ToString()));
				continue;
			}

			result_streams.emplace_back(std::move(stream));
		}
	}

	if (result_streams.empty()) {
		return exec_result;
	}

	// Phase 5: Combine results.
	auto result = result_merger->CollectAndMergeResults(result_streams, names, types, query_analysis);

	// Calculate worker execution time (from start to end of worker operations)
	auto worker_end = std::chrono::high_resolution_clock::now();
	exec_result.worker_execution_time = 
		std::chrono::duration_cast<std::chrono::milliseconds>(worker_end - worker_start);

	exec_result.result = std::move(result);
	return exec_result;
}

bool DistributedExecutor::CanDistribute(const string &sql) {
	string sql_upper = StringUtil::Upper(sql);
	StringUtil::Trim(sql_upper);

	// Must be a SELECT query
	if (!StringUtil::StartsWith(sql_upper, "SELECT ")) {
		return false;
	}

	// Must have a data source to partition
	if (sql_upper.find(" FROM ") == string::npos) {
		return false;
	}

	// ORDER BY requires global ordering - problematic for distributed execution
	// (would need to collect all data, then sort)
	if (sql_upper.find(" ORDER BY ") != string::npos) {
		return false;
	}

	// LIMIT without ORDER BY could work, but OFFSET is tricky in distributed context
	// TODO: Could support LIMIT by having deriver stop after N rows collected.
	if (sql_upper.find(" OFFSET ") != string::npos) {
		return false;
	}

	return true;
}

} // namespace duckdb
