#include "server/validation.hpp"

#include "client.pb.h"
#include "distributed.pb.h"
#include "transaction.pb.h"

namespace duckdb {

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
