#include "server/driver/worker_node_client.hpp"

namespace duckdb {

WorkerNodeClient::WorkerNodeClient(const string &location_p) : location(std::move(location_p)) {
}

arrow::Status WorkerNodeClient::Connect() {
	arrow::flight::Location flight_location;
	ARROW_ASSIGN_OR_RAISE(flight_location, arrow::flight::Location::Parse(location));
	ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(flight_location));
	return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<arrow::flight::FlightStreamReader>>
WorkerNodeClient::ExecutePartition(const distributed::ExecutePartitionRequest &request) {
	distributed::DistributedRequest req;
	*req.mutable_execute_partition() = request;

	arrow::flight::Ticket ticket;
	ticket.ticket = req.SerializeAsString();
	return client->DoGet(ticket);
}

} // namespace duckdb
