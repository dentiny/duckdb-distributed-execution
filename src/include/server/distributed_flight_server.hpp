#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "server/distributed_executor.hpp"
#include "server/worker_manager.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <memory>

namespace duckdb {

// Arrow Flight-based RPC server for distributed execution.
class DistributedFlightServer : public arrow::flight::FlightServerBase {
public:
	explicit DistributedFlightServer(string host_p = "0.0.0.0", int port_p = 8815);
	~DistributedFlightServer() override = default;

	// Start the server.
	arrow::Status Start();

	// Start with worker nodes
	arrow::Status StartWithWorkers(idx_t num_workers);

	// Stop the server.
	void Shutdown();

	// Get server location.
	string GetLocation() const;

	// Flight RPC methods.
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

	arrow::Status DoPut(const arrow::flight::ServerCallContext &context,
	                    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
	                    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

private:
	// Process different request types using protobuf messages directly.
	arrow::Status HandleExecuteSQL(const distributed::ExecuteSQLRequest &req, distributed::DistributedResponse &resp);

	// Handle CREATE TABLE request.
	// Return error status if the table already exists.
	arrow::Status HandleCreateTable(const distributed::CreateTableRequest &req, distributed::DistributedResponse &resp);

	// Handle DROP TABLE request.
	// Return OK status if the table doesn't exist.
	arrow::Status HandleDropTable(const distributed::DropTableRequest &req, distributed::DistributedResponse &resp);

	// Handle CREATE INDEX request.
	// Return error status if the index already exists.
	arrow::Status HandleCreateIndex(const distributed::CreateIndexRequest &req, distributed::DistributedResponse &resp);

	// Handle DROP INDEX request.
	// Return OK status if the index doesn't exist.
	arrow::Status HandleDropIndex(const distributed::DropIndexRequest &req, distributed::DistributedResponse &resp);

	// Handle ALTER TABLE request.
	// Return error status if the table doesn't exist or if the alteration fails.
	arrow::Status HandleAlterTable(const distributed::AlterTableRequest &req, distributed::DistributedResponse &resp);

	// Handle LOAD EXTENSION request.
	// Return error status if the extension fails to load.
	arrow::Status HandleLoadExtension(const distributed::LoadExtensionRequest &req,
	                                  distributed::DistributedResponse &resp);

	arrow::Status HandleTableExists(const distributed::TableExistsRequest &req, distributed::DistributedResponse &resp);
	arrow::Status HandleScanTable(const distributed::ScanTableRequest &req,
	                              std::unique_ptr<arrow::flight::FlightDataStream> &stream);
	arrow::Status HandleInsertData(const std::string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                               distributed::DistributedResponse &resp);

	// Convert DuckDB result to Arrow RecordBatch.
	arrow::Status QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader,
	                                 idx_t *row_count = nullptr);

private:
	string host;
	int port;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> conn;
	unique_ptr<WorkerManager> worker_manager;
	unique_ptr<DistributedExecutor> distributed_executor;
};

} // namespace duckdb
