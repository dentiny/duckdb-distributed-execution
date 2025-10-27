// TODO(hjiang): Current server is populating fake data, the real implementation is populating with table creation and
// insertion.

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

// Simple distributed server that manages multiple DuckDB instances
class DistributedServer {
public:
	DistributedServer();
	~DistributedServer() = default;

	// Get the singleton instance
	static DistributedServer &GetInstance();

	// Initialize server with a DuckDB instance.
	void Initialize();

	// Execute arbitrary SQL on the server
	unique_ptr<QueryResult> ExecuteSQL(const string &sql);

	// Get table data (simulating distributed scan).
	unique_ptr<QueryResult> ScanTable(const string &table_name, idx_t limit = 1000, idx_t offset = 0);

	// Check if table exists.
	bool TableExists(const string &table_name);

	// CREATE TABLE on server
	unique_ptr<QueryResult> CreateTable(const string &create_sql);

	// INSERT INTO on server
	unique_ptr<QueryResult> InsertInto(const string &insert_sql);

private:
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> conn;
};

} // namespace duckdb
