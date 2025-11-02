#pragma once

#include "distributed_flight_client.hpp"
#include "distributed_protocol.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

// Client wrapper for distributed execution
//
// This wraps DistributedFlightClient to provide a simple API for catalog operations.
// All operations send protobuf messages via gRPC to the Flight server and block for responses.
//
// Architecture:
// 1. start_distributed_server() SQL function → Starts gRPC Flight server in thread
// 2. DistributedClient::GetInstance() → Creates DistributedFlightClient
// 3. CreateTable/ScanTable/etc → Sends protobuf messages via gRPC to Flight server
// 4. Blocks waiting for protobuf responses
// 5. Flight server executes and returns results
class DistributedClient {
public:
	DistributedClient(const string &server_url = "grpc://localhost:8815");
	~DistributedClient() = default;

	// Get the singleton instance
	static DistributedClient &GetInstance();

	// Initialize and connect to Flight server
	void Initialize();

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
	string server_url_;
	unique_ptr<DistributedFlightClient> client_;
	bool connected_ = false;
};

} // namespace duckdb
