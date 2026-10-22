#include "server/driver/worker_node_client.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include <arrow/buffer.h>
#include <arrow/flight/client.h>

namespace duckdb {

WorkerNodeClient::WorkerNodeClient(const string &location_p) : location(std::move(location_p)) {
}

WorkerNodeClient::~WorkerNodeClient() = default;

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

arrow::Result<std::unique_ptr<arrow::flight::FlightStreamReader>>
WorkerNodeClient::ReadExchange(const string &exchange_id, idx_t bucket) {
	distributed::DistributedRequest req;
	auto &read = *req.mutable_read_exchange();
	read.set_exchange_id(exchange_id);
	read.set_bucket(NumericCast<uint32_t>(bucket));

	arrow::flight::Ticket ticket;
	ticket.ticket = req.SerializeAsString();
	return client->DoGet(ticket);
}

arrow::Status WorkerNodeClient::ReleaseExchange(const string &exchange_id) {
	distributed::DistributedRequest req;
	req.mutable_release_exchange()->set_exchange_id(exchange_id);

	arrow::flight::Action action;
	action.type = "release_exchange";
	action.body = arrow::Buffer::FromString(req.SerializeAsString());
	ARROW_ASSIGN_OR_RAISE(auto results, client->DoAction(action));
	return results->Drain();
}

} // namespace duckdb
