// Arrow Flight-based RPC client for distributed execution.

#pragma once

#include "distributed.pb.h"
#include "distributed_protocol.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/query_result.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <memory>

namespace duckdb {

struct ScanTableOptions {
	string session_id;
	bool include_rowid = false;
	bool project_columns = false;
	vector<string> projected_columns;
	uint32_t protocol_version = DUCKHERDER_PROTOCOL_VERSION;
};

class DistributedFlightClient {
public:
	explicit DistributedFlightClient(string server_url);
	~DistributedFlightClient() = default;

	// Connect to server.
	arrow::Status Connect();

	// Execute arbitrary SQL.
	arrow::Status ExecuteSQL(const string &sql, distributed::DistributedResponse &response,
	                         const string &session_id = "");

	// Open/close a server-side session backed by a dedicated DuckDB connection.
	arrow::Status OpenSession(string &session_id, uint32_t protocol_version = DUCKHERDER_PROTOCOL_VERSION);
	arrow::Status CloseSession(const string &session_id, distributed::DistributedResponse &response);

	// Create table.
	arrow::Status CreateTable(const string &create_sql, distributed::DistributedResponse &response);

	// Drop table.
	arrow::Status DropTable(const string &drop_sql, distributed::DistributedResponse &response);

	// Create index.
	arrow::Status CreateIndex(const string &create_sql, distributed::DistributedResponse &response);

	// Drop index.
	arrow::Status DropIndex(const string &index_name, distributed::DistributedResponse &response);

	// Load extension.
	arrow::Status LoadExtension(const string &extension_name, const string &repository, const string &version,
	                            distributed::DistributedResponse &response);

	// Check if table exists.
	arrow::Status TableExists(const string &table_name, bool &exists);

	// Insert data using Arrow RecordBatch.
	arrow::Status InsertData(const string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                         distributed::DistributedResponse &response);

	// Scan table and get Arrow Flight stream
	arrow::Status ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
	                        std::unique_ptr<arrow::flight::FlightStreamReader> &stream,
	                        const ScanTableOptions &options = {});

	// Get query execution statistics from the server
	arrow::Status GetQueryExecutionStats(distributed::DistributedResponse &response);

private:
	// RPC implementation to send request and block wait response.
	arrow::Status SendAction(const distributed::DistributedRequest &req, distributed::DistributedResponse &resp);

private:
	string server_url;
	arrow::flight::Location location;
	std::unique_ptr<arrow::flight::FlightClient> client;
};

} // namespace duckdb
