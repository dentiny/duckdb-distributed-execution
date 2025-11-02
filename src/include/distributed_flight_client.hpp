#pragma once

#include "distributed_protocol.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/query_result.hpp"

#include <arrow/flight/client.h>
#include <arrow/record_batch.h>
#include <memory>

namespace duckdb {

// Arrow Flight-based RPC client for distributed execution
class DistributedFlightClient {
public:
	explicit DistributedFlightClient(const string &server_url);
	~DistributedFlightClient() = default;

	// Connect to server
	arrow::Status Connect();

	// Execute arbitrary SQL
	arrow::Status ExecuteSQL(const string &sql, DistributedResponse &response);

	// Create table
	arrow::Status CreateTable(const string &create_sql, DistributedResponse &response);

	// Drop table
	arrow::Status DropTable(const string &drop_sql, DistributedResponse &response);

	// Check if table exists
	arrow::Status TableExists(const string &table_name, bool &exists);

	// Insert data using Arrow RecordBatch
	arrow::Status InsertData(const string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                         DistributedResponse &response);

	// Scan table and get Arrow Flight stream
	arrow::Status ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
	                        std::unique_ptr<arrow::flight::FlightStreamReader> &stream);

private:
	arrow::Status SendAction(const DistributedRequest &req, DistributedResponse &resp);

private:
	string server_url_;
	arrow::flight::Location location_;
	std::unique_ptr<arrow::flight::FlightClient> client_;
};

} // namespace duckdb
