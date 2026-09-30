#include "server/validation.hpp"

#include "client.pb.h"
#include "distributed.pb.h"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage.pb.h"
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
		if (!config.backend().empty() || !config.root().empty()) {
			return arrow::Status::Invalid("Object storage settings require a database");
		}
		return arrow::Status::OK();
	}
	if (!StringUtil::StartsWith(config.database_uri(), OBJFS_SCHEME) ||
	    config.database_uri().size() == string(OBJFS_SCHEME).size()) {
		return arrow::Status::Invalid("Object storage database must be a named duckdb_objfs:// URI");
	}
	// Memory backends are private to one DuckDB instance, so workers could never see the control node's data.
	if (config.backend() != "local") {
		return arrow::Status::Invalid("Object storage backend must be 'local'");
	}
	if (config.root().empty() || !LocalFileSystem().IsPathAbsolute(config.root())) {
		return arrow::Status::Invalid("Local object storage root must be an absolute path");
	}
	return arrow::Status::OK();
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
