#include "distributed_server.hpp"

#include <iostream>

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

DistributedServer::DistributedServer() {
	Initialize();
}

DistributedServer &DistributedServer::GetInstance() {
	static DistributedServer instance;
	return instance;
}

void DistributedServer::Initialize() {
	// Only initialize once
	if (db && conn) {
		return;
	}

	// Create DuckDB instance for the server.
	db = make_uniq<DuckDB>();
	conn = make_uniq<Connection>(*db);

	std::cout << "✅ Distributed server initialized (empty database ready)" << std::endl;
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

unique_ptr<QueryResult> DistributedServer::ExecuteSQL(const string &sql) {
	std::cout << "📡 Server executing SQL: " << sql << std::endl;
	return conn->Query(sql);
}

unique_ptr<QueryResult> DistributedServer::CreateTable(const string &create_sql) {
	std::cout << "🏗️  Server creating table: " << create_sql << std::endl;
	return conn->Query(create_sql);
}

unique_ptr<QueryResult> DistributedServer::DropTable(const string &drop_sql) {
	std::cout << "🗑️  Server dropping table: " << drop_sql << std::endl;
	return conn->Query(drop_sql);
}

unique_ptr<QueryResult> DistributedServer::InsertInto(const string &insert_sql) {
	std::cout << "💾 Server inserting data: " << insert_sql << std::endl;
	return conn->Query(insert_sql);
}

} // namespace duckdb
