#include "client/transport/distributed_flight_client.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"
#include "utils/retry_utils.hpp"

#include <arrow/buffer.h>

namespace duckdb {

DistributedFlightClient::DistributedFlightClient(string server_url_p, distributed::ClientRole role_p,
                                                 optional_ptr<DatabaseInstance> db_instance_p)
    : server_url(std::move(server_url_p)), role(role_p), db_instance(db_instance_p) {
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
	transaction_id = 0;
	next_transaction_id = 1;
	next_request_sequence = 1;
	transaction_requires_rollback = false;
	pending_transaction_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
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
	if (action == distributed::TRANSACTION_ACTION_BEGIN && transaction_id != 0 &&
	    pending_transaction_action != distributed::TRANSACTION_ACTION_UNSPECIFIED) {
		distributed::DistributedRequest recovery_request;
		recovery_request.mutable_transaction()->set_action(pending_transaction_action);
		recovery_request.set_transaction_id(transaction_id);
		recovery_request.set_request_sequence(next_request_sequence);
		distributed::DistributedResponse recovery_response;
		auto retry_config = db_instance ? GetRetryConfig(*db_instance) : GetDefaultRetryConfig();
		auto recovery_status = RetryWithExponentialBackoff(
		    [&]() {
			    recovery_response.Clear();
			    return SendAction(recovery_request, recovery_response);
		    },
		    retry_config);
		if (!recovery_status.ok()) {
			return arrow::Status::Invalid(StringUtil::Format(
			    "Previous remote transaction outcome remains unresolved: %s", recovery_status.ToString()));
		}
		if (!recovery_response.has_transaction() ||
		    recovery_response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
			return arrow::Status::Invalid("Previous remote transaction outcome remains unresolved");
		}
		if (pending_transaction_action == distributed::TRANSACTION_ACTION_BEGIN) {
			distributed::DistributedRequest rollback_request;
			rollback_request.mutable_transaction()->set_action(distributed::TRANSACTION_ACTION_ROLLBACK);
			rollback_request.set_transaction_id(transaction_id);
			rollback_request.set_request_sequence(next_request_sequence + 1);
			distributed::DistributedResponse rollback_response;
			auto rollback_status = RetryWithExponentialBackoff(
			    [&]() {
				    rollback_response.Clear();
				    return SendAction(rollback_request, rollback_response);
			    },
			    retry_config);
			if (!rollback_status.ok() || !rollback_response.success()) {
				return arrow::Status::Invalid("Previous remote BEGIN could not be rolled back");
			}
		}
		next_transaction_id++;
		transaction_id = 0;
		next_request_sequence = 1;
		transaction_requires_rollback = false;
		pending_transaction_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
	}
	if (action == distributed::TRANSACTION_ACTION_BEGIN) {
		if (transaction_id != 0) {
			return arrow::Status::Invalid("A remote transaction is already active or has an unresolved outcome");
		}
		transaction_id = next_transaction_id;
		next_request_sequence = 1;
		transaction_requires_rollback = false;
		pending_transaction_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
	} else if (transaction_id == 0) {
		return arrow::Status::Invalid("No remote transaction is active");
	} else if (action == distributed::TRANSACTION_ACTION_COMMIT && transaction_requires_rollback) {
		return arrow::Status::Invalid(
		    "A remote operation has an unresolved outcome; the transaction must be rolled back");
	}

	distributed::DistributedRequest req;
	req.mutable_transaction()->set_action(action);
	req.set_transaction_id(transaction_id);
	req.set_request_sequence(next_request_sequence);

	// Replaying the same client-generated transaction identifier is idempotent.
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : GetDefaultRetryConfig();
	auto status = RetryWithExponentialBackoff(
	    [&]() {
		    response.Clear();
		    return SendAction(req, response);
	    },
	    retry_config);
	if (!status.ok()) {
		pending_transaction_action = action;
		return status;
	}
	next_request_sequence++;

	if (!response.success()) {
		if (response.has_transaction() && response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
			next_request_sequence--;
			pending_transaction_action = action;
			return status;
		}
		if (action == distributed::TRANSACTION_ACTION_BEGIN ||
		    (response.has_transaction() &&
		     response.transaction().status() != distributed::TRANSACTION_STATUS_UNKNOWN)) {
			if (action != distributed::TRANSACTION_ACTION_BEGIN) {
				next_transaction_id++;
			}
			transaction_id = 0;
			next_request_sequence = 1;
			transaction_requires_rollback = false;
			pending_transaction_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
		}
		return status;
	}
	if (action == distributed::TRANSACTION_ACTION_COMMIT || action == distributed::TRANSACTION_ACTION_ROLLBACK) {
		next_transaction_id++;
		transaction_id = 0;
		next_request_sequence = 1;
		transaction_requires_rollback = false;
		pending_transaction_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
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
	const lock_guard<mutex> lock(transaction_mutex);
	if (transaction_id == 0) {
		return arrow::Status::Invalid("No remote transaction is active");
	}
	auto request_transaction_id = transaction_id;
	auto request_sequence = next_request_sequence++;
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : GetDefaultRetryConfig();
	auto status = RetryWithExponentialBackoff(
	    [&]() {
		    response.Clear();
		    auto descriptor = arrow::flight::FlightDescriptor::Path({client_id, table_name,
		                                                             StringUtil::Format("%llu", request_transaction_id),
		                                                             StringUtil::Format("%llu", request_sequence)});
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
	if (!status.ok()) {
		transaction_requires_rollback = true;
	}
	return status;
}

arrow::Status DistributedFlightClient::ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
                                                 vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	const lock_guard<mutex> lock(transaction_mutex);
	if (transaction_id == 0) {
		return arrow::Status::Invalid("No remote transaction is active");
	}
	distributed::DistributedRequest req;
	auto *scan_req = req.mutable_scan_table();
	scan_req->set_table_name(table_name);
	scan_req->set_limit(limit);
	scan_req->set_offset(offset);
	req.set_client_id(client_id);
	req.set_transaction_id(transaction_id);
	req.set_request_sequence(next_request_sequence++);

	std::string req_data = req.SerializeAsString();
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : GetDefaultRetryConfig();
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
	if (!status.ok()) {
		transaction_requires_rollback = true;
	}
	return status;
}

arrow::Status DistributedFlightClient::GetQueryExecutionStats(distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	req.mutable_get_query_execution_stats();
	return SendIdempotentAction(req, response);
}

arrow::Status DistributedFlightClient::SendIdempotentAction(distributed::DistributedRequest &req,
                                                            distributed::DistributedResponse &resp) {
	const lock_guard<mutex> lock(transaction_mutex);
	if (transaction_id == 0) {
		return arrow::Status::Invalid("No remote transaction is active");
	}
	req.set_transaction_id(transaction_id);
	req.set_request_sequence(next_request_sequence++);
	auto retry_config = db_instance ? GetRetryConfig(*db_instance) : GetDefaultRetryConfig();
	auto status = RetryWithExponentialBackoff(
	    [&]() {
		    resp.Clear();
		    return SendAction(req, resp);
	    },
	    retry_config);
	if (!status.ok()) {
		transaction_requires_rollback = true;
	}
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
