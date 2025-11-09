#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <arrow/flight/api.h>
#include <memory>

namespace duckdb {

// STEP 3: Task execution state tracking
// This represents the execution state for a pipeline task on a worker
struct TaskExecutionState {
	idx_t task_id;
	idx_t total_tasks;
	string task_sql;
	idx_t rows_processed;
	idx_t execution_time_ms;
	bool completed;
	string error_message;

	TaskExecutionState() : task_id(0), total_tasks(0), rows_processed(0), execution_time_ms(0), completed(false) {
	}
};

// Simple worker node that executes queries on partitioned data.
class WorkerNode : public arrow::flight::FlightServerBase {
public:
	WorkerNode(string worker_id_p, string host_p = "0.0.0.0", int port_p = 0, DuckDB *shared_db = nullptr);
	~WorkerNode() override = default;

	arrow::Status Start();
	void Shutdown();
	string GetLocation() const;
	string GetWorkerId() const {
		return worker_id;
	}
	int GetPort() const {
		return port;
	}

	// Flight RPC methods
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

private:
	arrow::Status HandleExecutePartition(const distributed::ExecutePartitionRequest &req,
	                                     distributed::DistributedResponse &resp,
	                                     std::shared_ptr<arrow::RecordBatchReader> &reader);
	arrow::Status ExecuteSerializedPlan(const distributed::ExecutePartitionRequest &req,
	                                    unique_ptr<QueryResult> &result);
	arrow::Status QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader,
	                                 idx_t *row_count = nullptr);

	// STEP 3: Execute a pipeline task with state tracking
	arrow::Status ExecutePipelineTask(const distributed::ExecutePartitionRequest &req, TaskExecutionState &task_state,
	                                  unique_ptr<QueryResult> &result);

	string worker_id;
	string host;
	int port;
	DuckDB *db;
	unique_ptr<DuckDB> owned_db;
	unique_ptr<Connection> conn;

	// STEP 3: Track currently executing task (for monitoring/debugging)
	unique_ptr<TaskExecutionState> current_task;
};

// Simple client for worker communication.
class WorkerNodeClient {
public:
	explicit WorkerNodeClient(const string &location);
	arrow::Status Connect();
	arrow::Status ExecutePartition(const distributed::ExecutePartitionRequest &request,
	                               std::unique_ptr<arrow::flight::FlightStreamReader> &stream);

private:
	string location;
	std::unique_ptr<arrow::flight::FlightClient> client;
};

} // namespace duckdb
