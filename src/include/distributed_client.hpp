// Client implementation to remote execution server.

#pragma once

#include "distributed_flight_client.hpp"
#include "distributed_protocol.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DistributedClient {
public:
	explicit DistributedClient(string server_url_p = "grpc://localhost:8815");
	~DistributedClient() = default;

	static DistributedClient &GetInstance();

	// Execute arbitrary SQL on the server (via protobuf ExecuteSQLRequest)
	unique_ptr<QueryResult> ExecuteSQL(const string &sql);

	// Check if table exists (via protobuf TableExistsRequest)
	bool TableExists(const string &table_name);

	// CREATE TABLE on server (via protobuf CreateTableRequest)
	unique_ptr<QueryResult> CreateTable(const string &create_sql);

	// DROP TABLE on server (via protobuf DropTableRequest)
	unique_ptr<QueryResult> DropTable(const string &drop_sql);

	// INSERT INTO on server (via protobuf ExecuteSQLRequest)
	unique_ptr<QueryResult> InsertInto(const string &insert_sql);

	// Get table data (via protobuf ScanTableRequest → Arrow RecordBatches)
	unique_ptr<QueryResult> ScanTable(const string &table_name, idx_t limit = 1000, idx_t offset = 0);

private:
	string server_url;
	unique_ptr<DistributedFlightClient> client;
	bool connected = false;
};

} // namespace duckdb
