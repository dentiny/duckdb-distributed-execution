#include "server/object_storage_database.hpp"

#include "core_functions_extension.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

namespace {

constexpr const char *OBJECT_STORAGE_CATALOG = "object_db";
constexpr const char *DEFAULT_DATABASE_URI = "duckdb_objfs://__duckherder_internal_default__";

arrow::Status Execute(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		return arrow::Status::IOError("Object storage initialization failed on '", sql, "': ", result->GetError());
	}
	return arrow::Status::OK();
}

arrow::Status ConfigureObjectStorage(Connection &conn, const distributed::StorageConfig &config) {
	switch (config.storage_case()) {
	case distributed::StorageConfig::kInMemory:
		return Execute(conn, "SET GLOBAL duckdb_objfs_backend = 'memory'");
	case distributed::StorageConfig::kLocal:
		ARROW_RETURN_NOT_OK(Execute(conn, "SET GLOBAL duckdb_objfs_backend = 'local'"));
		return Execute(conn, StringUtil::Format("SET GLOBAL duckdb_objfs_root = %s",
		                                        KeywordHelper::WriteQuoted(config.local().root())));
	case distributed::StorageConfig::kS3:
		return arrow::Status::NotImplemented("Duckherder does not support S3 object storage yet");
	default:
		return arrow::Status::Invalid("Object storage configuration must specify a storage type");
	}
}

} // namespace

distributed::StorageConfig ObjectStorageDatabase::ResolveConfig(const distributed::StorageConfig &config) {
	if (!config.database_uri().empty()) {
		return config;
	}
	distributed::StorageConfig result;
	result.set_database_uri(DEFAULT_DATABASE_URI);
	result.mutable_in_memory();
	return result;
}

bool ObjectStorageDatabase::IsDefaultURI(const string &database_uri) {
	return database_uri == DEFAULT_DATABASE_URI;
}

string ObjectStorageDatabase::GetKey(const distributed::StorageConfig &config) {
	return config.SerializeAsString();
}

ObjectStorageDatabase::ObjectStorageDatabase(shared_ptr<DuckDB> instance_p) : instance(std::move(instance_p)) {
}

arrow::Result<unique_ptr<ObjectStorageDatabase>> ObjectStorageDatabase::Create(const distributed::StorageConfig &config,
                                                                               AccessMode access_mode) {
	try {
		auto instance = make_shared_ptr<DuckDB>(/*path=*/nullptr, /*config=*/nullptr);
		instance->LoadStaticExtension<CoreFunctionsExtension>();
		Connection conn(*instance);
		ARROW_RETURN_NOT_OK(Execute(conn, "LOAD duckdb_object_storage"));
		ARROW_RETURN_NOT_OK(ConfigureObjectStorage(conn, config));
		ARROW_RETURN_NOT_OK(
		    Execute(conn, StringUtil::Format("ATTACH %s AS %s%s", KeywordHelper::WriteQuoted(config.database_uri()),
		                                     OBJECT_STORAGE_CATALOG,
		                                     access_mode == AccessMode::READ_ONLY ? " (READ_ONLY)" : "")));
		return unique_ptr<ObjectStorageDatabase>(new ObjectStorageDatabase(std::move(instance)));
	} catch (const std::exception &ex) {
		return arrow::Status::IOError(ErrorData(ex).Message());
	}
}

arrow::Result<unique_ptr<Connection>> ObjectStorageDatabase::Connect() const {
	try {
		auto conn = make_uniq<Connection>(*instance);
		ARROW_RETURN_NOT_OK(Execute(*conn, StringUtil::Format("USE %s", OBJECT_STORAGE_CATALOG)));
		return std::move(conn);
	} catch (const std::exception &ex) {
		return arrow::Status::IOError(ErrorData(ex).Message());
	}
}

DuckDB &ObjectStorageDatabase::GetInstance() const {
	return *instance;
}

const shared_ptr<DuckDB> &ObjectStorageDatabase::GetSharedInstance() const {
	return instance;
}

} // namespace duckdb
