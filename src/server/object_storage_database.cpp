#include "server/object_storage_database.hpp"

#include "core_functions_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

namespace {

void ExecuteOrThrow(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		throw IOException("Object storage initialization failed on '%s': %s", sql, result->GetError());
	}
}

} // namespace

bool HasObjectStorage(const distributed::StorageConfig &config) {
	return !config.database_uri().empty();
}

string GetStorageKey(const distributed::StorageConfig &config) {
	return config.SerializeAsString();
}

unique_ptr<DuckDB> OpenObjectStorageDatabase(const distributed::StorageConfig &config, AccessMode access_mode) {
	auto db = make_uniq<DuckDB>(/*path=*/nullptr, /*config=*/nullptr);
	db->LoadStaticExtension<CoreFunctionsExtension>();
	Connection conn(*db);
	ExecuteOrThrow(conn, "LOAD duckdb_object_storage");
	ExecuteOrThrow(
	    conn, StringUtil::Format("SET GLOBAL duckdb_objfs_backend = %s", KeywordHelper::WriteQuoted(config.backend())));
	ExecuteOrThrow(conn,
	               StringUtil::Format("SET GLOBAL duckdb_objfs_root = %s", KeywordHelper::WriteQuoted(config.root())));
	ExecuteOrThrow(conn, StringUtil::Format("ATTACH %s AS %s%s", KeywordHelper::WriteQuoted(config.database_uri()),
	                                        OBJECT_STORAGE_CATALOG,
	                                        access_mode == AccessMode::READ_ONLY ? " (READ_ONLY)" : ""));
	return db;
}

unique_ptr<Connection> ConnectObjectStorageDatabase(DuckDB &db) {
	auto conn = make_uniq<Connection>(db);
	ExecuteOrThrow(*conn, StringUtil::Format("USE %s", OBJECT_STORAGE_CATALOG));
	return conn;
}

} // namespace duckdb
