#include "server/object_storage_database.hpp"

#include "core_functions_extension.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "server/startup_sql.hpp"

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

string S3Scope(const distributed::S3Storage &config) {
	auto scope = StringUtil::Format("s3://%s", config.bucket());
	return config.root().empty() ? scope : StringUtil::Format("%s/%s", scope, config.root());
}

arrow::Status RegisterS3Secret(Connection &conn, const distributed::S3Storage &config) {
	bool transaction_active = false;
	try {
		conn.BeginTransaction();
		transaction_active = true;
		auto &secret_manager = SecretManager::Get(*conn.context);
		SecretType secret_type;
		secret_type.name = "s3";
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		secret_manager.RegisterSecretType(secret_type);

		auto secret = make_uniq<KeyValueSecret>(vector<string> {S3Scope(config)}, "s3", "config", "__duckherder_s3");
		if (!config.key_id().empty()) {
			secret->secret_map["key_id"] = Value(config.key_id());
			secret->secret_map["secret"] = Value(config.secret());
		}
		if (!config.session_token().empty()) {
			secret->secret_map["session_token"] = Value(config.session_token());
		}
		if (!config.endpoint().empty()) {
			secret->secret_map["endpoint"] = Value(config.endpoint());
		}
		if (!config.region().empty()) {
			secret->secret_map["region"] = Value(config.region());
		}
		if (config.has_use_ssl()) {
			secret->secret_map["use_ssl"] = Value::BOOLEAN(config.use_ssl());
		}
		switch (config.url_style()) {
		case distributed::S3_URL_STYLE_PATH:
			secret->secret_map["url_style"] = Value("path");
			break;
		case distributed::S3_URL_STYLE_VHOST:
			secret->secret_map["url_style"] = Value("vhost");
			break;
		default:
			break;
		}
		secret->redact_keys = {"secret", "session_token"};

		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*conn.context);
		secret_manager.RegisterSecret(transaction, std::move(secret), OnCreateConflict::ERROR_ON_CONFLICT,
		                              SecretPersistType::TEMPORARY);
		conn.Commit();
		transaction_active = false;
		return arrow::Status::OK();
	} catch (const std::exception &ex) {
		if (transaction_active) {
			try {
				conn.Rollback();
			} catch (...) {
			}
		}
		return arrow::Status::Invalid(ErrorData(ex).Message());
	}
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
		ARROW_RETURN_NOT_OK(RegisterS3Secret(conn, config.s3()));
		ARROW_RETURN_NOT_OK(Execute(conn, "SET GLOBAL duckdb_objfs_backend = 's3'"));
		ARROW_RETURN_NOT_OK(Execute(conn, StringUtil::Format("SET GLOBAL duckdb_objfs_bucket = %s",
		                                                     KeywordHelper::WriteQuoted(config.s3().bucket()))));
		return Execute(conn, StringUtil::Format("SET GLOBAL duckdb_objfs_root = %s",
		                                        KeywordHelper::WriteQuoted(config.s3().root())));
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
	auto identity = config;
	if (identity.storage_case() == distributed::StorageConfig::kS3) {
		auto s3 = identity.mutable_s3();
		s3->clear_key_id();
		s3->clear_secret();
		s3->clear_session_token();
	}
	return identity.SerializeAsString();
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
		// Arrow 1.5 encodes decimals at their physical width instead of always as 128-bit, shrinking scan results.
		ARROW_RETURN_NOT_OK(Execute(conn, "SET GLOBAL arrow_output_version = '1.5'"));
		ARROW_RETURN_NOT_OK(ConfigureObjectStorage(conn, config));
		ARROW_RETURN_NOT_OK(RunStartupSQL(*instance));
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
