#include "server/driver/worker_node_client.hpp"

namespace duckdb {

WorkerNodeClient::WorkerNodeClient(const string &location) : location(location) {
}

arrow::Status WorkerNodeClient::Connect() {
	arrow::flight::Location flight_location;
	ARROW_ASSIGN_OR_RAISE(flight_location, arrow::flight::Location::Parse(location));
	ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(flight_location));
	
	// Verify the connection actually works by calling a dummy action.
	// FlightClient::Connect() is lazy and doesn't establish a real network connection.
	// This forces an actual network round-trip and will fail if the server isn't running.
	// The "dummy_connect" action does nothing except confirm the worker is reachable.
	arrow::flight::Action action;
	action.type = "dummy_connect";
	action.body = arrow::Buffer::FromString("");
	
	auto result = client->DoAction(action);
	if (!result.ok()) {
		return arrow::Status::IOError("Failed to connect to worker at " + location + ": " + result.status().ToString());
	}
	
	// Consume at least one result to ensure the action actually executed
	std::unique_ptr<arrow::flight::ResultStream> stream = std::move(*result);
	auto next_result = stream->Next();
	if (!next_result.ok()) {
		return arrow::Status::IOError("Failed to verify worker connection at " + location + ": " + next_result.status().ToString());
	}
	
	return arrow::Status::OK();
}

arrow::Status WorkerNodeClient::ExecutePartition(const distributed::ExecutePartitionRequest &request,
                                                 std::unique_ptr<arrow::flight::FlightStreamReader> &stream) {
	distributed::DistributedRequest req;
	*req.mutable_execute_partition() = request;

	std::string req_data = req.SerializeAsString();
	arrow::flight::Action action {"execute_partition", arrow::Buffer::FromString(req_data)};

	auto action_result = client->DoAction(action);
	if (!action_result.ok()) {
		return action_result.status();
	}
	auto result_stream = std::move(action_result).ValueOrDie();

	auto next_result = result_stream->Next();
	if (!next_result.ok()) {
		return next_result.status();
	}
	auto result = std::move(next_result).ValueOrDie();
	if (!result) {
		return arrow::Status::Invalid("No response from worker");
	}

	distributed::DistributedResponse response;
	if (!response.ParseFromArray(result->body->data(), result->body->size())) {
		return arrow::Status::Invalid("Failed to parse response");
	}

	if (!response.success()) {
		return arrow::Status::Invalid("Worker execution failed: " + response.error_message());
	}

	// Now get the actual data stream using DoGet with the request as ticket.
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;
	auto doget_result = client->DoGet(ticket);
	if (!doget_result.ok()) {
		return doget_result.status();
	}
	stream = std::move(doget_result).ValueOrDie();

	return arrow::Status::OK();
}

} // namespace duckdb

