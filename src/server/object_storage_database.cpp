#include "server/object_storage_database.hpp"

#include "core_functions_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

namespace {

constexpr const char *OBJECT_STORAGE_CATALOG = "object_db";

void ExecuteOrThrow(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		throw IOException("Object storage initialization failed on '%s': %s", sql, result->GetError());
	}
}

void ConfigureObjectStorage(Connection &conn, const distributed::StorageConfig &config) {
	switch (config.storage_case()) {
	case distributed::StorageConfig::kInMemory:
		ExecuteOrThrow(conn, "SET GLOBAL duckdb_objfs_backend = 'memory'");
		return;
	case distributed::StorageConfig::kLocal:
		ExecuteOrThrow(conn, "SET GLOBAL duckdb_objfs_backend = 'local'");
		ExecuteOrThrow(conn, StringUtil::Format("SET GLOBAL duckdb_objfs_root = %s",
		                                        KeywordHelper::WriteQuoted(config.local().root())));
		return;
	case distributed::StorageConfig::kS3:
		throw NotImplementedException("Duckherder does not support S3 object storage yet");
	default:
		throw InvalidInputException("Object storage configuration must specify a storage type");
	}
}

} // namespace

bool ObjectStorageDatabase::IsConfigured(const distributed::StorageConfig &config) {
	return !config.database_uri().empty();
}

string ObjectStorageDatabase::GetKey(const distributed::StorageConfig &config) {
	return config.SerializeAsString();
}

ObjectStorageDatabase::ObjectStorageDatabase(const distributed::StorageConfig &config, AccessMode access_mode)
    : instance(make_shared_ptr<DuckDB>(/*path=*/nullptr, /*config=*/nullptr)) {
	instance->LoadStaticExtension<CoreFunctionsExtension>();
	Connection conn(*instance);
	ExecuteOrThrow(conn, "LOAD duckdb_object_storage");
	ConfigureObjectStorage(conn, config);
	ExecuteOrThrow(conn, StringUtil::Format("ATTACH %s AS %s%s", KeywordHelper::WriteQuoted(config.database_uri()),
	                                        OBJECT_STORAGE_CATALOG,
	                                        access_mode == AccessMode::READ_ONLY ? " (READ_ONLY)" : ""));
}

unique_ptr<Connection> ObjectStorageDatabase::Connect() const {
	auto conn = make_uniq<Connection>(*instance);
	ExecuteOrThrow(*conn, StringUtil::Format("USE %s", OBJECT_STORAGE_CATALOG));
	return conn;
}

DuckDB &ObjectStorageDatabase::GetInstance() const {
	return *instance;
}

const shared_ptr<DuckDB> &ObjectStorageDatabase::GetSharedInstance() const {
	return instance;
}

} // namespace duckdb
