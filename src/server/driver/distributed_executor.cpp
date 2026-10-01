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
#include "duckdb/parallel/task_executor.hpp"
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
#include "server/driver/query_utils.hpp"
#include "server/driver/worker_manager.hpp"

namespace duckdb {

namespace {

class WorkerDispatchTask : public BaseExecutorTask {
public:
	WorkerDispatchTask(TaskExecutor &executor, WorkerNodeClient &client_p, const vector<idx_t> &task_indices_p,
	                   const vector<distributed::ExecutePartitionRequest> &requests_p,
	                   vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &result_streams_p,
	                   vector<arrow::Status> &task_statuses_p)
	    : BaseExecutorTask(executor), client(client_p), task_indices(task_indices_p), requests(requests_p),
	      result_streams(result_streams_p), task_statuses(task_statuses_p) {
	}

	void ExecuteTask() override {
		for (auto task_idx : task_indices) {
			task_statuses[task_idx] = client.ExecutePartition(requests[task_idx], result_streams[task_idx]);
			if (!task_statuses[task_idx].ok()) {
				return;
			}
		}
	}

	string TaskType() const override {
		return "WorkerDispatchTask";
	}

private:
	WorkerNodeClient &client;
	const vector<idx_t> &task_indices;
	const vector<distributed::ExecutePartitionRequest> &requests;
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &result_streams;
	vector<arrow::Status> &task_statuses;
};

} // namespace

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p,
                                         distributed::StorageConfig storage_config_p)
    : worker_manager(worker_manager_p), conn(conn_p), storage_config(std::move(storage_config_p)) {
	plan_analyzer = make_uniq<QueryPlanAnalyzer>(conn);
	result_merger = make_uniq<ResultMerger>(conn);
	task_partitioner = make_uniq<TaskPartitioner>(conn, *plan_analyzer);
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

	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return exec_result;
	}

	// Phase 1: Plan extraction and validation
	unique_ptr<LogicalOperator> logical_plan = conn.ExtractPlan(sql);
	if (logical_plan == nullptr) {
		return exec_result;
	}
	if (!IsSupportedPlan(*logical_plan)) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Logical plan for query '%s' contains unsupported operators", sql));
		return exec_result;
	}

	// Analyze query to determine merge strategy
	QueryPlanAnalyzer::QueryAnalysis query_analysis = plan_analyzer->AnalyzeQuery(*logical_plan, statement);
	const bool partitioned_aggregation = query_analysis.supports_partitioned_aggregation &&
	                                     storage_config.storage_case() != distributed::StorageConfig::STORAGE_NOT_SET;
	// The partial query scans the same table with the same filters, so it is partitioned with the original plan.
	const string &execution_sql = partitioned_aggregation ? query_analysis.partial_sql : sql;

	// Phase 2: Extract pipeline tasks and distribute to workers
	// This replaces the old 1-partition-per-worker approach with flexible task distribution
	const idx_t partition_workers = query_analysis.has_aggregation && !partitioned_aggregation ? 1 : workers.size();
	auto tasks = task_partitioner->ExtractPipelineTasks(*logical_plan, execution_sql, partition_workers);
	if (tasks.empty()) {
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

	// Map tasks to workers using round-robin
	// The partitioner creates at most one task per worker; tables with fewer row groups than workers yield M < N.
	// Maps from worker_id -> [task_indices]
	vector<vector<idx_t>> worker_to_tasks(workers.size());
	for (idx_t idx = 0; idx < tasks.size(); ++idx) {
		const idx_t worker_id = idx % workers.size();
		worker_to_tasks[worker_id].emplace_back(idx);
	}

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
	vector<string> serialized_types;
	serialized_types.reserve(partial_types.size());
	for (const auto &type : partial_types) {
		serialized_types.emplace_back(PlanSerializer::SerializeLogicalType(type));
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
		for (const auto &name : partial_names) {
			req.add_column_names(name);
		}
		for (const auto &type_bytes : serialized_types) {
			req.add_column_types(type_bytes);
		}
	}

	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams(tasks.size());
	vector<arrow::Status> task_statuses(tasks.size());
	TaskExecutor executor(*conn.context);
	for (idx_t worker_id = 0; worker_id < workers.size(); ++worker_id) {
		if (worker_to_tasks[worker_id].empty()) {
			continue;
		}
		executor.ScheduleTask(make_uniq<WorkerDispatchTask>(executor, *workers[worker_id]->client,
		                                                    worker_to_tasks[worker_id], requests, result_streams,
		                                                    task_statuses));
	}
	executor.WorkOnTasks();

	for (idx_t worker_id = 0; worker_id < workers.size(); ++worker_id) {
		for (auto task_idx : worker_to_tasks[worker_id]) {
			const auto &status = task_statuses[task_idx];
			if (!status.ok()) {
				// Merging the remaining partitions would silently drop this task's rows.
				DUCKDB_LOG_WARNING(db_instance,
				                   StringUtil::Format("Worker %s failed executing task %llu, falling back to local "
				                                      "execution: %s",
				                                      workers[worker_id]->worker_id,
				                                      static_cast<long long unsigned>(tasks[task_idx].task_id),
				                                      status.ToString()));
				return exec_result;
			}
		}
	}

	// Phase 5: Combine results.
	if (partitioned_aggregation) {
		exec_result.result = result_merger->MergePartialAggregates(result_streams, partial_names, partial_types, names,
		                                                           types, query_analysis.final_sql);
		return exec_result;
	}
	std::shared_ptr<arrow::Schema> schema;
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto collect_status = result_merger->CollectResults(result_streams, names, types, schema, batches);
	if (collect_status.IsInvalid()) {
		exec_result.result = make_uniq<MaterializedQueryResult>(ErrorData(
		    InternalException(StringUtil::Format("Failed collecting worker results: %s", collect_status.ToString()))));
		return exec_result;
	}
	if (!collect_status.ok()) {
		DUCKDB_LOG_WARNING(db_instance, StringUtil::Format("Failed collecting worker results, falling back to local "
		                                                   "execution: %s",
		                                                   collect_status.ToString()));
		return exec_result;
	}
	exec_result.arrow_schema = std::move(schema);
	exec_result.arrow_batches = std::move(batches);
	return exec_result;
}

} // namespace duckdb
