#include "client/transport/distributed_flight_client.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "utils/retry_utils.hpp"

#include <arrow/buffer.h>

namespace duckdb {

DistributedFlightClient::DistributedFlightClient(string server_url_p, distributed::ClientRole role_p,
                                                 optional_ptr<DatabaseInstance> db_instance_p)
    : server_url(std::move(server_url_p)), role(role_p), db_instance(db_instance_p) {
	InitTransactionState();
}

DistributedFlightClient::~DistributedFlightClient() {
	Close();
}

arrow::Status DistributedFlightClient::Connect() {
	if (client && !client_id.empty()) {
		return arrow::Status::OK();
	}
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::Parse(server_url));
	ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));
	auto status = RegisterClient();
	if (!status.ok()) {
		client.reset();
		return status;
	}
	stop_heartbeat = false;
	heartbeat_thread = std::thread(&DistributedFlightClient::HeartbeatLoop, this);
	return status;
}

void DistributedFlightClient::Close() {
	stop_heartbeat = true;
	heartbeat_cv.notify_all();
	if (heartbeat_thread.joinable()) {
		heartbeat_thread.join();
	}
	UnregisterClientNoThrow();

	const lock_guard<mutex> lock(transaction_mutex);
	InitTransactionState();
}

void DistributedFlightClient::SetTransactionContext(optional_ptr<ClientContext> context) {
	transaction_state.context = context;
}

bool DistributedFlightClient::HasActiveTransaction() {
	const lock_guard<mutex> lock(transaction_mutex);
	return transaction_state.transaction_id != INVALID_TRANSACTION_ID;
}

arrow::Status DistributedFlightClient::EnsureExplicitTransaction() {
	{
		const lock_guard<mutex> lock(transaction_mutex);
		if (transaction_state.pending_action != distributed::TRANSACTION_ACTION_UNSPECIFIED) {
			distributed::DistributedResponse response;
			ARROW_RETURN_NOT_OK(ResolvePendingTransaction(response));
		}
	}
	if (!transaction_state.context || transaction_state.context->transaction.IsAutoCommit() || HasActiveTransaction()) {
		return arrow::Status::OK();
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
		    return SendAction(request, response);
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

arrow::Status DistributedFlightClient::RegisterClient() {
	distributed::DistributedRequest req;
	req.mutable_register_client()->set_role(role);

	distributed::DistributedResponse response;
	ARROW_RETURN_NOT_OK(SendAction(req, response));
	if (!response.success()) {
		return arrow::Status::Invalid(response.error_message());
	}
	if (!response.has_register_client() || response.register_client().client_id().empty()) {
		return arrow::Status::Invalid("Control node returned an invalid client registration");
	}
	client_id = response.register_client().client_id();
	return arrow::Status::OK();
}

void DistributedFlightClient::UnregisterClientNoThrow() {
	if (!client || client_id.empty()) {
		return;
	}

	try {
		distributed::DistributedRequest req;
		req.mutable_unregister_client();
		distributed::DistributedResponse response;
		(void)SendAction(req, response);
	} catch (...) {
		// Destruction and DETACH must remain non-throwing if the server is gone.
	}
	client_id.clear();
}

void DistributedFlightClient::HeartbeatLoop() {
	unique_lock<mutex> lock(heartbeat_mutex);
	while (!stop_heartbeat) {
		if (heartbeat_cv.wait_for(lock, std::chrono::seconds(10), [this] { return stop_heartbeat.load(); })) {
			break;
		}
		lock.unlock();
		distributed::DistributedRequest req;
		req.mutable_client_heartbeat();
		distributed::DistributedResponse response;
		(void)SendAction(req, response);
		lock.lock();
	}
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
	lock_guard<mutex> lock(transaction_mutex);
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
	const lock_guard<mutex> lock(transaction_mutex);
	distributed::DistributedRequest identity_request;
	auto identity = AssignRequestIdentity(identity_request);
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : RetryConfig();
	auto status = RetryWithExponentialBackoff(
	    [&]() {
		    response.Clear();
		    auto descriptor = arrow::flight::FlightDescriptor::Path(
		        {client_id, table_name, StringUtil::Format("%llu", identity.transaction_id),
		         StringUtil::Format("%llu", identity.request_sequence),
		         StringUtil::Format("%d", static_cast<int>(identity.mode))});
		    ARROW_ASSIGN_OR_RAISE(auto put_result, client->DoPut(descriptor, batch->schema()));
		    ARROW_RETURN_NOT_OK(put_result.writer->WriteRecordBatch(*batch));
		    ARROW_RETURN_NOT_OK(put_result.writer->DoneWriting());

		    std::shared_ptr<arrow::Buffer> metadata;
		    ARROW_RETURN_NOT_OK(put_result.reader->ReadMetadata(&metadata));
		    if (metadata == nullptr) {
			    return arrow::Status::Invalid("No response from server");
		    }
		    if (!response.ParseFromArray(metadata->data(), metadata->size())) {
			    return arrow::Status::Invalid("Failed to parse response");
		    }
		    return arrow::Status::OK();
	    },
	    retry_config);
	FinishRequest(identity, status);
	return status;
}

arrow::Status DistributedFlightClient::ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
                                                 vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	ARROW_RETURN_NOT_OK(EnsureExplicitTransaction());
	const lock_guard<mutex> lock(transaction_mutex);
	distributed::DistributedRequest req;
	auto *scan_req = req.mutable_scan_table();
	scan_req->set_table_name(table_name);
	scan_req->set_limit(limit);
	scan_req->set_offset(offset);
	req.set_client_id(client_id);
	auto identity = AssignRequestIdentity(req);

	std::string req_data = req.SerializeAsString();
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : RetryConfig();
	auto status = RetryWithExponentialBackoff(
	    [&]() {
		    vector<std::shared_ptr<arrow::RecordBatch>> attempt_batches;
		    ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(ticket));
		    while (true) {
			    ARROW_ASSIGN_OR_RAISE(auto next, stream->Next());
			    if (!next.data) {
				    break;
			    }
			    attempt_batches.emplace_back(std::move(next.data));
		    }
		    batches = std::move(attempt_batches);
		    return arrow::Status::OK();
	    },
	    retry_config);
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
	const lock_guard<mutex> lock(transaction_mutex);
	auto identity = AssignRequestIdentity(req);
	auto status = SendActionWithRetry(req, resp);
	FinishRequest(identity, status);
	return status;
}

arrow::Status DistributedFlightClient::SendAction(const distributed::DistributedRequest &req,
                                                  distributed::DistributedResponse &resp) {
	auto request = req;
	if (!client_id.empty()) {
		request.set_client_id(client_id);
	}
	std::string req_data = request.SerializeAsString();

	arrow::flight::Action action;
	action.type = "execute";
	action.body = arrow::Buffer::FromString(req_data);

	// Send action and get results
	std::unique_ptr<arrow::flight::ResultStream> results;
	ARROW_ASSIGN_OR_RAISE(results, client->DoAction(action));

	std::unique_ptr<arrow::flight::Result> result;
	ARROW_ASSIGN_OR_RAISE(result, results->Next());

	if (result == nullptr) {
		return arrow::Status::Invalid("No response from server");
	}
	if (!resp.ParseFromArray(result->body->data(), result->body->size())) {
		return arrow::Status::Invalid("Failed to parse response");
	}

	return arrow::Status::OK();
}

} // namespace duckdb
