#include "server/driver/distributed_executor.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/worker_manager.hpp"

#include <arrow/util/future.h>

namespace duckdb {

namespace {

// Runs requests on workers and stores their results in task_batches. Returns false if any task fails.
bool RunTasks(WorkerManager &worker_manager, ClientContext &context, const vector<WorkerInfo *> &workers,
              const vector<distributed::ExecutePartitionRequest> &requests,
              vector<arrow::RecordBatchVector> &task_batches) {
	auto &db_instance = *context.db;
	// Each task runs on its own worker. Tasks planned for workers that have since left run locally instead.
	if (requests.size() > workers.size()) {
		DUCKDB_LOG_DEBUG(*context.db, StringUtil::Format("%llu tasks but %llu workers, falling back to local execution",
		                                                 requests.size(), workers.size()));
		return false;
	}
	task_batches.assign(requests.size(), {});
	Value thread_setting;
	if (!db_instance.TryGetCurrentSetting("duckherder_async_threads", thread_setting)) {
		throw InternalException("Duckherder async thread setting is not registered");
	}
	auto thread_count = thread_setting.GetValue<int64_t>();
	if (thread_count == 0) {
		const auto cpu_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		thread_count = MinValue<idx_t>(cpu_threads, 64) * 4;
	}
	auto pool_result = worker_manager.GetDispatchPool(NumericCast<int>(thread_count));
	if (!pool_result.ok()) {
		throw IOException("Failed creating worker dispatch pool: %s", pool_result.status().ToString());
	}
	auto pool = *pool_result;
	vector<arrow::Future<>> dispatches;
	arrow::Status dispatch_error = arrow::Status::OK();
	dispatches.reserve(requests.size());
	for (idx_t task_idx = 0; task_idx < requests.size(); ++task_idx) {
		// Flight calls block while the worker runs; keep their waits outside
		// DuckDB's CPU-sized task pool.
		auto submitted = pool->Submit([&, task_idx]() -> arrow::Status {
			return workers[task_idx]->client->ExecutePartition(requests[task_idx], task_batches[task_idx]);
		});
		if (!submitted.ok()) {
			dispatch_error = submitted.status();
			break;
		}
		dispatches.emplace_back(*submitted);
	}
	// Complete every submitted task before returning or throwing: their captures
	// refer to this stack frame.
	for (auto &dispatch : dispatches) {
		dispatch.Wait();
	}
	if (!dispatch_error.ok()) {
		throw IOException("Worker dispatch failed: %s", dispatch_error.ToString());
	}
	for (idx_t task_idx = 0; task_idx < dispatches.size(); ++task_idx) {
		const auto &status = dispatches[task_idx].status();
		if (!status.ok()) {
			// Merging the remaining partitions would silently drop this task's rows.
			DUCKDB_LOG_WARNING(db_instance,
			                   StringUtil::Format("Worker %s failed executing task %llu, falling back to local "
			                                      "execution: %s",
			                                      workers[task_idx]->worker_id,
			                                      static_cast<long long unsigned>(task_idx), status.ToString()));
			return false;
		}
	}
	return true;
}

} // namespace

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p,
                                         distributed::StorageConfig storage_config_p)
    : worker_manager(worker_manager_p), conn(conn_p), storage_config(std::move(storage_config_p)) {
	plan_analyzer = make_uniq<QueryPlanAnalyzer>(conn);
	result_merger = make_uniq<ResultMerger>(conn);
	task_partitioner = make_uniq<TaskPartitioner>(conn, *plan_analyzer);
}

bool DistributedExecutor::CanPartitionJoin(LogicalOperator &plan, const string &sql) {
	const auto num_workers = worker_manager.GetWorkerCount();
	if (storage_config.storage_case() == distributed::StorageConfig::STORAGE_NOT_SET || num_workers < 2 ||
	    !IsSupportedPlan(plan)) {
		return false;
	}
	Parser parser;
	try {
		parser.ParseQuery(sql);
	} catch (const ParserException &) {
		return false;
	}
	return parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT &&
	       task_partitioner->CanPartitionJoin(plan, parser.statements[0]->Cast<SelectStatement>(), num_workers);
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
DistributedExecutionResult DistributedExecutor::ExecuteDistributed(const string &sql, DistributedFragmentKind kind) {
	DistributedExecutionResult exec_result;
	auto &db_instance = *conn.context->db;

	// Which operators can be distributed is checked on the plan.
	Parser parser;
	try {
		parser.ParseQuery(sql);
	} catch (const ParserException &) {
		// Local execution reports the syntax error to the client.
		return exec_result;
	}
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return exec_result;
	}
	const auto &statement = parser.statements[0]->Cast<SelectStatement>();
	if (kind == DistributedFragmentKind::PARTITIONED_JOIN && !IsSimplePartitionedJoin(statement)) {
		return exec_result;
	}

	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return exec_result;
	}

	// Phase 1: Plan extraction and validation
	// Refresh the plan for task partitioning; table storage may change after the client prepares the fragment.
	unique_ptr<LogicalOperator> logical_plan = conn.ExtractPlan(sql);
	if (logical_plan == nullptr) {
		return exec_result;
	}
	if (!IsSupportedPlan(*logical_plan)) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Logical plan for query '%s' contains unsupported operators", sql));
		return exec_result;
	}

	QueryPlanAnalyzer::QueryAnalysis query_analysis = QueryPlanAnalyzer::AnalyzeQuery(*logical_plan, statement);
	const bool partitioned_aggregation = query_analysis.supports_partitioned_aggregation &&
	                                     storage_config.storage_case() != distributed::StorageConfig::STORAGE_NOT_SET;
	if (kind == DistributedFragmentKind::PARTITIONED_JOIN && !partitioned_aggregation) {
		return exec_result;
	}
	// The partial query scans the same table with the same filters, so it is partitioned with the original plan.
	const string &execution_sql = partitioned_aggregation ? query_analysis.partial_sql : sql;

	// Phase 2: Extract pipeline tasks and distribute to workers
	// This replaces the old 1-partition-per-worker approach with flexible task distribution
	const idx_t partition_workers = query_analysis.has_aggregation && !partitioned_aggregation ? 1 : workers.size();
	auto tasks = task_partitioner->ExtractPipelineTasks(*logical_plan, execution_sql, partition_workers);
	if (tasks.empty()) {
		return exec_result;
	}
	// Recheck at execution time: one task would reread the other input without parallelizing the Join.
	if (kind == DistributedFragmentKind::PARTITIONED_JOIN && tasks.size() < 2) {
		return exec_result;
	}

	// A delegated query already contains its final result, including aggregates.
	if (tasks.size() == 1 && !partitioned_aggregation) {
		query_analysis.merge_strategy = QueryPlanAnalyzer::MergeStrategy::CONCATENATE;
	}
	exec_result.merge_strategy = query_analysis.merge_strategy;

	exec_result.num_tasks = tasks.size();
	exec_result.num_workers_used = tasks.size();

	exec_result.partition_strategy = tasks.size() == 1 ? PartitionStrategy::NONE : PartitionStrategy::ROW_GROUP_ALIGNED;

	// Phase 3: Prepare result schema and type information。
	auto prepared = conn.Prepare(sql);
	if (prepared->HasError()) {
		// Propagate the error instead of continuing
		exec_result.result = make_uniq<MaterializedQueryResult>(prepared->GetErrorObject());
		return exec_result;
	}

	vector<string> names = prepared->GetNames();
	vector<LogicalType> types = prepared->GetTypes();
	vector<string> partial_names = names;
	vector<LogicalType> partial_types = types;
	if (partitioned_aggregation) {
		auto partial_prepared = conn.Prepare(query_analysis.partial_sql);
		if (partial_prepared->HasError()) {
			throw InternalException("Failed to prepare partial aggregate query '%s': %s", query_analysis.partial_sql,
			                        partial_prepared->GetError());
		}
		partial_names = partial_prepared->GetNames();
		partial_types = partial_prepared->GetTypes();
	}

	// Phase 4: Distribute tasks to workers。
	vector<distributed::ExecutePartitionRequest> requests(tasks.size());
	for (idx_t task_idx = 0; task_idx < tasks.size(); ++task_idx) {
		auto &task = tasks[task_idx];
		auto &req = requests[task_idx];
		req.set_sql(task.task_sql);
		req.set_partition_id(task.task_id);
		req.set_total_partitions(task.total_tasks);
		*req.mutable_storage_config() = storage_config;
	}

	vector<arrow::RecordBatchVector> task_batches;
	if (!RunTasks(worker_manager, *conn.context, workers, requests, task_batches)) {
		return exec_result;
	}

	// Phase 5: Combine results.
	if (partitioned_aggregation) {
		exec_result.result = result_merger->MergePartialAggregates(task_batches, partial_names, partial_types, names,
		                                                           types, query_analysis.final_sql);
		return exec_result;
	}
	std::shared_ptr<arrow::Schema> schema;
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto collect_status = result_merger->CollectResults(task_batches, names, types, schema, batches);
	if (!collect_status.ok()) {
		exec_result.result = make_uniq<MaterializedQueryResult>(ErrorData(
		    InternalException(StringUtil::Format("Failed collecting worker results: %s", collect_status.ToString()))));
		return exec_result;
	}
	exec_result.arrow_schema = std::move(schema);
	exec_result.arrow_batches = std::move(batches);
	return exec_result;
}

idx_t DistributedExecutor::NumWorkers() {
	return worker_manager.GetWorkerCount();
}

// Distributed execution mirrors DuckDB's parallel execution model: each worker task runs the plan fragment on its
// rows like a thread fills its LocalSinkState, and the driver combines the task results like the GlobalSinkState.
bool DistributedExecutor::ExecutePlans(const vector<string> &plans, vector<arrow::RecordBatchVector> &task_batches) {
	auto &db_instance = *conn.context->db;
	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return false;
	}
	if (storage_config.storage_case() == distributed::StorageConfig::STORAGE_NOT_SET) {
		DUCKDB_LOG_DEBUG(db_instance, "No storage configured for workers, falling back to local execution");
		return false;
	}
	vector<distributed::ExecutePartitionRequest> requests(plans.size());
	for (idx_t task_idx = 0; task_idx < plans.size(); ++task_idx) {
		auto &req = requests[task_idx];
		req.set_serialized_plan(plans[task_idx]);
		req.set_partition_id(task_idx);
		req.set_total_partitions(plans.size());
		*req.mutable_storage_config() = storage_config;
		req.set_build_id(PlanSerializer::BuildId());
	}
	return RunTasks(worker_manager, *conn.context, workers, requests, task_batches);
}

} // namespace duckdb
