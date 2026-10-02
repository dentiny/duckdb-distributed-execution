#include "client/transport/flight_client_session.hpp"

#include "duckdb/common/string_util.hpp"

#include <arrow/buffer.h>

namespace duckdb {

FlightClientSession::FlightClientSession(string server_url_p, distributed::ClientRole role_p,
                                         distributed::StorageConfig storage_config_p)
    : server_url(std::move(server_url_p)), role(role_p), storage_config(std::move(storage_config_p)) {
}

FlightClientSession::~FlightClientSession() {
	Close();
}

arrow::Status FlightClientSession::Connect() {
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
	heartbeat_thread = std::thread(&FlightClientSession::HeartbeatLoop, this);
	return status;
}

void FlightClientSession::Close() {
	stop_heartbeat = true;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(heartbeat_mutex);
		heartbeat_cv.notify_all();
	}
	if (heartbeat_thread.joinable()) {
		heartbeat_thread.join();
	}
	UnregisterClientNoThrow();
}

arrow::Status FlightClientSession::RegisterClient() {
	distributed::DistributedRequest req;
	req.mutable_register_client()->set_role(role);
	*req.mutable_register_client()->mutable_storage_config() = storage_config;

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

void FlightClientSession::UnregisterClientNoThrow() {
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

void FlightClientSession::HeartbeatLoop() {
	concurrency::unique_lock<concurrency::mutex> lock(heartbeat_mutex);
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

arrow::Status FlightClientSession::SendAction(const distributed::DistributedRequest &req,
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

arrow::Status FlightClientSession::DoGet(const distributed::DistributedRequest &req,
                                         vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	auto request = req;
	request.set_client_id(client_id);
	arrow::flight::Ticket ticket;
	ticket.ticket = request.SerializeAsString();

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
}

arrow::Status FlightClientSession::DoPut(const string &table_name, const distributed::DistributedRequest &identity,
                                         const std::shared_ptr<arrow::RecordBatch> &batch,
                                         distributed::DistributedResponse &response) {
	auto descriptor = arrow::flight::FlightDescriptor::Path(
	    {client_id, table_name, StringUtil::Format("%llu", identity.transaction_id()),
	     StringUtil::Format("%llu", identity.request_sequence()),
	     StringUtil::Format("%d", static_cast<int>(identity.transaction_mode()))});
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
}

} // namespace duckdb
