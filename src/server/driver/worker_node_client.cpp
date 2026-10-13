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

arrow::Status WorkerNodeClient::ExecutePartition(const distributed::ExecutePartitionRequest &request,
                                                 arrow::RecordBatchVector &batches) {
	distributed::DistributedRequest req;
	*req.mutable_execute_partition() = request;

	arrow::flight::Ticket ticket;
	ticket.ticket = req.SerializeAsString();
	ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(ticket));
	ARROW_ASSIGN_OR_RAISE(batches, stream->ToRecordBatches());
	return arrow::Status::OK();
}

} // namespace duckdb
