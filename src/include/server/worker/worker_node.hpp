#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <arrow/flight/api.h>
#include <memory>

namespace duckdb {

// Simple worker node that executes queries on partitioned data
class WorkerNode : public arrow::flight::FlightServerBase {
public:
    explicit WorkerNode(string worker_id_p, string host_p = "0.0.0.0", int port_p = 0,
                        DuckDB *shared_db = nullptr);
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

	string worker_id;
	string host;
	int port;
    DuckDB *db;
    unique_ptr<DuckDB> owned_db;
	unique_ptr<Connection> conn;
};

// Simple client for worker communication
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
