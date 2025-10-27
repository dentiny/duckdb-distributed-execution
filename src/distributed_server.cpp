#include "distributed_server.hpp"

#include <iostream>

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

DistributedServer::DistributedServer() {
	Initialize();
}

void DistributedServer::Initialize() {
	// Create DuckDB instance for the server.
	db = make_uniq<DuckDB>();
	conn = make_uniq<Connection>(*db);

	// Create some test data.
	auto result = conn->Query("CREATE TABLE IF NOT EXISTS my_table (id INTEGER, name VARCHAR, value INTEGER)");
	if (result->HasError()) {
		std::cerr << "Error creating table: " << result->GetError() << std::endl;
		return;
	}

	// Insert test data.
	result = conn->Query(
	    "INSERT INTO my_table VALUES (1, 'Server_Alice', 100), (2, 'Server_Bob', 200), (3, 'Server_Charlie', 300)");
	if (result->HasError()) {
		std::cerr << "Error inserting data: " << result->GetError() << std::endl;
	}

	std::cout << "Distributed server initialized with test data" << std::endl;
}

unique_ptr<QueryResult> DistributedServer::ExecuteQuery(const string &sql) {
	std::cout << "Server executing: " << sql << std::endl;
	return conn->Query(sql);
}

unique_ptr<QueryResult> DistributedServer::ScanTable(const string &table_name, idx_t limit, idx_t offset) {
	string sql = StringUtil::Format("SELECT * FROM %s LIMIT %d OFFSET %d", table_name, limit, offset);
	std::cout << "Server scanning table: " << sql << std::endl;
	return conn->Query(sql);
}

bool DistributedServer::TableExists(const string &table_name) {
	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", table_name);
	auto result = conn->Query(sql);
	if (result->HasError()) {
		return false;
	}

	if (result->Fetch()) {
		return result->GetValue(0, 0).GetValue<int>() > 0;
	}
	return false;
}

} // namespace duckdb
