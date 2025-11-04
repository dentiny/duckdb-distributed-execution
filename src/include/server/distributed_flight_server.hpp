#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <arrow/flight/server.h>
#include <arrow/record_batch.h>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace duckdb {

// Arrow Flight-based RPC server for distributed execution.
class DistributedFlightServer : public arrow::flight::FlightServerBase {
public:
	explicit DistributedFlightServer(string host_p = "0.0.0.0", int port_p = 8815);
	~DistributedFlightServer() override = default;

	// Start the server.
	arrow::Status Start();

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
	// Get or create a connection for the specified database path.
	Connection &GetConnection(const string &db_path);

	// Process different request types using protobuf messages directly.
	arrow::Status HandleExecuteSQL(const string &db_path, const distributed::ExecuteSQLRequest &req,
	                               distributed::DistributedResponse &resp);

	// Handle CREATE TABLE request.
	// Return error status if the table already exists.
	arrow::Status HandleCreateTable(const string &db_path, const distributed::CreateTableRequest &req,
	                                distributed::DistributedResponse &resp);

	// Handle DROP TABLE request.
	// Return OK status if the table doesn't exist.
	arrow::Status HandleDropTable(const string &db_path, const distributed::DropTableRequest &req,
	                              distributed::DistributedResponse &resp);

	// Handle CREATE INDEX request.
	// Return error status if the index already exists.
	arrow::Status HandleCreateIndex(const string &db_path, const distributed::CreateIndexRequest &req,
	                                distributed::DistributedResponse &resp);

	// Handle DROP INDEX request.
	// Return OK status if the index doesn't exist.
	arrow::Status HandleDropIndex(const string &db_path, const distributed::DropIndexRequest &req,
	                              distributed::DistributedResponse &resp);

	// Handle ALTER TABLE request.
	// Return error status if the table doesn't exist or if the alteration fails.
	arrow::Status HandleAlterTable(const string &db_path, const distributed::AlterTableRequest &req,
	                               distributed::DistributedResponse &resp);

	arrow::Status HandleTableExists(const string &db_path, const distributed::TableExistsRequest &req,
	                                distributed::DistributedResponse &resp);
	arrow::Status HandleGetCatalogInfo(const string &db_path, const distributed::GetCatalogInfoRequest &req,
	                                   distributed::DistributedResponse &resp);
	arrow::Status HandleScanTable(const string &db_path, const distributed::ScanTableRequest &req,
	                              std::unique_ptr<arrow::flight::FlightDataStream> &stream);
	arrow::Status HandleInsertData(const string &db_path, const std::string &table_name,
	                               std::shared_ptr<arrow::RecordBatch> batch, distributed::DistributedResponse &resp);

	// Convert DuckDB result to Arrow RecordBatch.
	arrow::Status QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader);

private:
	struct DatabaseConnection {
		unique_ptr<DuckDB> db;
		unique_ptr<Connection> conn;
	};

	string host;
	int port;

	// Database connections cache, keyed by database path.
	std::mutex connections_mutex;
	std::unordered_map<string, unique_ptr<DatabaseConnection>> connections;
};

} // namespace duckdb
