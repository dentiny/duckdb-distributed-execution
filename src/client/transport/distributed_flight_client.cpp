#include "client/transport/distributed_flight_client.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "utils/retry_utils.hpp"

namespace duckdb {

DistributedFlightClient::DistributedFlightClient(string server_url_p, distributed::ClientRole role_p,
                                                 optional_ptr<DatabaseInstance> db_instance_p,
                                                 distributed::StorageConfig storage_config_p)
    : db_instance(db_instance_p), session(std::move(server_url_p), role_p, std::move(storage_config_p)) {
}

DistributedFlightClient::~DistributedFlightClient() {
	Close();
}

arrow::Status DistributedFlightClient::Connect() {
	return session.Connect();
}

void DistributedFlightClient::Close() {
	session.Close();

	const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	InitTransactionState();
}

void DistributedFlightClient::SetTransactionContext(optional_ptr<ClientContext> context) {
	const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	transaction_state.context = context;
}

bool DistributedFlightClient::HasActiveTransaction() {
	const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	return transaction_state.transaction_id != INVALID_TRANSACTION_ID;
}

arrow::Status DistributedFlightClient::EnsureExplicitTransaction() {
	{
		const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
		if (transaction_state.pending_action != distributed::TRANSACTION_ACTION_UNSPECIFIED) {
			distributed::DistributedResponse response;
			ARROW_RETURN_NOT_OK(ResolvePendingTransaction(response));
		}
		if (!transaction_state.context || transaction_state.context->transaction.IsAutoCommit() ||
		    transaction_state.transaction_id != INVALID_TRANSACTION_ID) {
			return arrow::Status::OK();
		}
	}
	distributed::DistributedResponse response;
	ARROW_RETURN_NOT_OK(ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response));
	if (!response.success()) {
		return arrow::Status::Invalid(response.error_message());
	}
	return arrow::Status::OK();
}

distributed::DistributedRequest DistributedFlightClient::CreateTransactionRequest(distributed::TransactionAction action,
                                                                                  uint64_t request_sequence) const {
	distributed::DistributedRequest request;
	request.mutable_transaction()->set_action(action);
	request.set_transaction_id(transaction_state.transaction_id);
	request.set_request_sequence(request_sequence);
	request.set_transaction_mode(distributed::TRANSACTION_MODE_EXPLICIT);
	return request;
}

arrow::Status DistributedFlightClient::SendActionWithRetry(const distributed::DistributedRequest &request,
                                                           distributed::DistributedResponse &response) {
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : RetryConfig();
	return RetryWithExponentialBackoff(
	    [&]() {
		    response.Clear();
		    return session.SendAction(request, response);
	    },
	    retry_config);
}

void DistributedFlightClient::InitTransactionState() {
	transaction_state = DistributedTransactionState();
}

arrow::Status DistributedFlightClient::ResolvePendingTransaction(distributed::DistributedResponse &response) {
	auto pending_request =
	    CreateTransactionRequest(transaction_state.pending_action, transaction_state.next_request_sequence);
	auto status = SendActionWithRetry(pending_request, response);
	if (!status.ok()) {
		return arrow::Status::Invalid(
		    StringUtil::Format("Previous remote transaction outcome remains unresolved: %s", status.ToString()));
	}
	if (!response.has_transaction() || response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
		return arrow::Status::Invalid("Previous remote transaction outcome remains unresolved");
	}

	if (transaction_state.pending_action == distributed::TRANSACTION_ACTION_BEGIN) {
		auto rollback_request = CreateTransactionRequest(distributed::TRANSACTION_ACTION_ROLLBACK,
		                                                 transaction_state.next_request_sequence + 1);
		status = SendActionWithRetry(rollback_request, response);
		if (!status.ok() || !response.success()) {
			return arrow::Status::Invalid("Previous remote BEGIN could not be rolled back");
		}
	}

	transaction_state.next_transaction_id++;
	transaction_state.ResetExplicitTransaction();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::ExecuteStatement(const string &sql, const string &client_catalog,
                                                        distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	auto *exec_req = req.mutable_execute_statement();
	exec_req->set_sql(sql);
	exec_req->set_client_catalog(client_catalog);
	return SendIdempotentAction(req, response);
}

arrow::Status DistributedFlightClient::ManageTransaction(distributed::TransactionAction action,
                                                         distributed::DistributedResponse &response) {
	concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	if (action == distributed::TRANSACTION_ACTION_BEGIN && transaction_state.transaction_id != INVALID_TRANSACTION_ID &&
	    transaction_state.pending_action != distributed::TRANSACTION_ACTION_UNSPECIFIED) {
		ARROW_RETURN_NOT_OK(ResolvePendingTransaction(response));
	}
	if (action == distributed::TRANSACTION_ACTION_ROLLBACK &&
	    transaction_state.pending_action == distributed::TRANSACTION_ACTION_BEGIN) {
		return ResolvePendingTransaction(response);
	}
	if (action == distributed::TRANSACTION_ACTION_BEGIN) {
		if (transaction_state.pending_autocommit_operation) {
			return arrow::Status::Invalid("Previous autocommit operation outcome remains unresolved");
		}
		if (transaction_state.transaction_id != INVALID_TRANSACTION_ID) {
			return arrow::Status::Invalid("A remote transaction is already active or has an unresolved outcome");
		}
		transaction_state.transaction_id = transaction_state.next_transaction_id;
		transaction_state.next_request_sequence = INITIAL_REQUEST_SEQUENCE;
		transaction_state.requires_rollback = false;
		transaction_state.pending_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
	} else if (transaction_state.transaction_id == INVALID_TRANSACTION_ID) {
		return arrow::Status::Invalid("No remote transaction is active");
	} else if (action == distributed::TRANSACTION_ACTION_COMMIT && transaction_state.requires_rollback) {
		return arrow::Status::Invalid(
		    "A remote operation has an unresolved outcome; the transaction must be rolled back");
	}

	auto request = CreateTransactionRequest(action, transaction_state.next_request_sequence);
	auto status = SendActionWithRetry(request, response);
	if (!status.ok()) {
		transaction_state.pending_action = action;
		return status;
	}
	if (!response.success() && response.has_transaction() &&
	    response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
		transaction_state.pending_action = action;
		return status;
	}
	transaction_state.next_request_sequence++;

	if (!response.success()) {
		if (action == distributed::TRANSACTION_ACTION_BEGIN ||
		    (response.has_transaction() &&
		     response.transaction().status() != distributed::TRANSACTION_STATUS_UNKNOWN)) {
			if (action != distributed::TRANSACTION_ACTION_BEGIN) {
				transaction_state.next_transaction_id++;
			}
			transaction_state.ResetExplicitTransaction();
		}
		return status;
	}
	if (action == distributed::TRANSACTION_ACTION_COMMIT || action == distributed::TRANSACTION_ACTION_ROLLBACK) {
		transaction_state.next_transaction_id++;
		transaction_state.ResetExplicitTransaction();
	}
	return status;
}

arrow::Status DistributedFlightClient::LoadExtension(const string &extension_name, const string &repository,
                                                     const string &version,
                                                     distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	auto *load_req = req.mutable_load_extension();
	load_req->set_extension_name(extension_name);
	if (!repository.empty()) {
		load_req->set_repository(repository);
	}
	if (!version.empty()) {
		load_req->set_version(version);
	}
	return SendIdempotentAction(req, response);
}

arrow::Status DistributedFlightClient::TableExists(const string &table_name, bool &exists) {
	distributed::DistributedRequest req;
	auto *exists_req = req.mutable_table_exists();
	exists_req->set_table_name(table_name);

	distributed::DistributedResponse resp;
	ARROW_RETURN_NOT_OK(SendIdempotentAction(req, resp));
	if (!resp.success()) {
		return arrow::Status::Invalid(resp.error_message());
	}

	exists = resp.table_exists().exists();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::InsertData(const string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
                                                  distributed::DistributedResponse &response) {
	ARROW_RETURN_NOT_OK(EnsureExplicitTransaction());
	const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	distributed::DistributedRequest identity_request;
	auto identity = AssignRequestIdentity(identity_request);
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : RetryConfig();
	auto status = RetryWithExponentialBackoff(
	    [&]() {
		    response.Clear();
		    return session.DoPut(table_name, identity_request, batch, response);
	    },
	    retry_config);
	FinishRequest(identity, status);
	return status;
}

arrow::Status DistributedFlightClient::ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
                                                 vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	ARROW_RETURN_NOT_OK(EnsureExplicitTransaction());
	const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	distributed::DistributedRequest req;
	auto *scan_req = req.mutable_scan_table();
	scan_req->set_table_name(table_name);
	scan_req->set_limit(limit);
	scan_req->set_offset(offset);
	auto identity = AssignRequestIdentity(req);

	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : RetryConfig();
	auto status = RetryWithExponentialBackoff([&]() { return session.DoGet(req, batches); }, retry_config);
	FinishRequest(identity, status);
	return status;
}

arrow::Status DistributedFlightClient::GetQueryExecutionStats(distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	req.mutable_get_query_execution_stats();
	return SendIdempotentAction(req, response);
}

DistributedFlightClient::RequestIdentity
DistributedFlightClient::AssignRequestIdentity(distributed::DistributedRequest &req) {
	RequestIdentity identity;
	if (transaction_state.transaction_id == INVALID_TRANSACTION_ID) {
		identity = {transaction_state.next_transaction_id, INITIAL_REQUEST_SEQUENCE,
		            distributed::TRANSACTION_MODE_AUTOCOMMIT};
	} else {
		identity = {transaction_state.transaction_id, transaction_state.next_request_sequence++,
		            distributed::TRANSACTION_MODE_EXPLICIT};
	}
	req.set_transaction_id(identity.transaction_id);
	req.set_request_sequence(identity.request_sequence);
	req.set_transaction_mode(identity.mode);
	return identity;
}

void DistributedFlightClient::FinishRequest(const RequestIdentity &identity, const arrow::Status &status) {
	if (identity.mode == distributed::TRANSACTION_MODE_AUTOCOMMIT) {
		if (status.ok()) {
			transaction_state.next_transaction_id++;
			transaction_state.pending_autocommit_operation = false;
		} else {
			transaction_state.pending_autocommit_operation = true;
		}
		return;
	}
	if (!status.ok()) {
		transaction_state.requires_rollback = true;
	}
}

arrow::Status DistributedFlightClient::SendIdempotentAction(distributed::DistributedRequest &req,
                                                            distributed::DistributedResponse &resp) {
	ARROW_RETURN_NOT_OK(EnsureExplicitTransaction());
	const concurrency::lock_guard<concurrency::mutex> lock(transaction_mutex);
	auto identity = AssignRequestIdentity(req);
	auto status = SendActionWithRetry(req, resp);
	FinishRequest(identity, status);
	return status;
}

} // namespace duckdb
