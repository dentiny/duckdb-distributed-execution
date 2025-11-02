#pragma once

#include "distributed_protocol.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <arrow/flight/server.h>
#include <arrow/record_batch.h>
#include <memory>

namespace duckdb {

// Arrow Flight-based RPC server for distributed execution
class DistributedFlightServer : public arrow::flight::FlightServerBase {
public:
	explicit DistributedFlightServer(const string &host = "0.0.0.0", int port = 8815);
	~DistributedFlightServer() override = default;

	// Start the server
	arrow::Status Start();

	// Stop the server
	void Shutdown();

	// Get server location
	string GetLocation() const;

	// Flight RPC methods
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

	arrow::Status DoPut(const arrow::flight::ServerCallContext &context,
	                    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
	                    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

private:
	// Process different request types using protobuf messages directly
	arrow::Status HandleExecuteSQL(const distributed::ExecuteSQLRequest &req, distributed::DistributedResponse &resp);
	arrow::Status HandleCreateTable(const distributed::CreateTableRequest &req, distributed::DistributedResponse &resp);
	arrow::Status HandleDropTable(const distributed::DropTableRequest &req, distributed::DistributedResponse &resp);
	arrow::Status HandleTableExists(const distributed::TableExistsRequest &req, distributed::DistributedResponse &resp);
	arrow::Status HandleScanTable(const distributed::ScanTableRequest &req,
	                              std::unique_ptr<arrow::flight::FlightDataStream> &stream);
	arrow::Status HandleInsertData(const std::string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                               distributed::DistributedResponse &resp);

	// Convert DuckDB result to Arrow RecordBatch
	arrow::Status QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader);

private:
	string host_;
	int port_;
	unique_ptr<DuckDB> db_;
	unique_ptr<Connection> conn_;
};

} // namespace duckdb
