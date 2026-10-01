#include "server/validation.hpp"

#include "client.pb.h"
#include "distributed.pb.h"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "server/object_storage_database.hpp"
#include "storage_config.pb.h"
#include "transaction.pb.h"

namespace duckdb {

namespace {

constexpr const char *OBJFS_SCHEME = "duckdb_objfs://";

} // namespace

arrow::Status ValidateRequest(const distributed::DistributedRequest &request) {
	if (request.request_case() == distributed::DistributedRequest::REQUEST_NOT_SET) {
		return arrow::Status::Invalid("Request type not set");
	}
	return arrow::Status::OK();
}

arrow::Status ValidateRequest(const distributed::RegisterClientRequest &request) {
	if (request.role() != distributed::CLIENT_ROLE_READ_ONLY && request.role() != distributed::CLIENT_ROLE_READ_WRITE) {
		return arrow::Status::Invalid("Duckherder client role must be specified");
	}
	return ValidateRequest(request.storage_config());
}

arrow::Status ValidateRequest(const distributed::StorageConfig &config) {
	if (config.database_uri().empty()) {
		if (config.storage_case() != distributed::StorageConfig::STORAGE_NOT_SET) {
			return arrow::Status::Invalid("Object storage settings require a database");
		}
		return arrow::Status::OK();
	}
	if (!StringUtil::StartsWith(config.database_uri(), OBJFS_SCHEME) ||
	    config.database_uri().size() == string(OBJFS_SCHEME).size()) {
		return arrow::Status::Invalid("Object storage database must be a named duckdb_objfs:// URI");
	}
	if (ObjectStorageDatabase::IsDefaultURI(config.database_uri())) {
		return arrow::Status::Invalid("Object storage database name is reserved for the server's default database");
	}
	switch (config.storage_case()) {
	case distributed::StorageConfig::kInMemory:
		return arrow::Status::OK();
	case distributed::StorageConfig::kLocal:
		if (config.local().root().empty() || !LocalFileSystem().IsPathAbsolute(config.local().root())) {
			return arrow::Status::Invalid("Local object storage root must be an absolute path");
		}
		return arrow::Status::OK();
	case distributed::StorageConfig::kS3:
		return arrow::Status::Invalid("Duckherder does not support S3 object storage yet");
	default:
		return arrow::Status::Invalid("Object storage database must specify a storage type");
	}
}

arrow::Status ValidateRequest(const distributed::TransactionRequest &request) {
	switch (request.action()) {
	case distributed::TRANSACTION_ACTION_BEGIN:
	case distributed::TRANSACTION_ACTION_COMMIT:
	case distributed::TRANSACTION_ACTION_ROLLBACK:
		return arrow::Status::OK();
	default:
		return arrow::Status::Invalid("Transaction action must be BEGIN, COMMIT, or ROLLBACK");
	}
}

} // namespace duckdb
