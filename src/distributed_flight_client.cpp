#include "distributed_flight_client.hpp"
#include "duckdb/common/string_util.hpp"

#include <arrow/buffer.h>
#include <iostream>

namespace duckdb {

DistributedFlightClient::DistributedFlightClient(const string &server_url) : server_url_(server_url) {
}

arrow::Status DistributedFlightClient::Connect() {
	ARROW_ASSIGN_OR_RAISE(location_, arrow::flight::Location::Parse(server_url_));
	ARROW_ASSIGN_OR_RAISE(client_, arrow::flight::FlightClient::Connect(location_));

	std::cout << "🔌 Connected to Flight server at " << server_url_ << std::endl;
	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::ExecuteSQL(const string &sql, DistributedResponse &response) {

	DistributedRequest req;
	req.type = RequestType::EXECUTE_SQL;
	req.sql = sql;

	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::CreateTable(const string &create_sql, DistributedResponse &response) {

	DistributedRequest req;
	req.type = RequestType::CREATE_TABLE;
	req.sql = create_sql;

	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::DropTable(const string &drop_sql, DistributedResponse &response) {

	DistributedRequest req;
	req.type = RequestType::DROP_TABLE;
	req.sql = drop_sql;

	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::TableExists(const string &table_name, bool &exists) {

	DistributedRequest req;
	req.type = RequestType::TABLE_EXISTS;
	req.table_name = table_name;

	DistributedResponse resp;
	ARROW_RETURN_NOT_OK(SendAction(req, resp));

	if (!resp.success) {
		return arrow::Status::Invalid(resp.error_message);
	}

	exists = resp.exists;
	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::InsertData(const string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
                                                  DistributedResponse &response) {

	DistributedRequest req;
	req.type = RequestType::INSERT_DATA;
	req.table_name = table_name;

	// Serialize request
	auto req_data = req.Serialize();
	auto descriptor = arrow::flight::FlightDescriptor::Command(
	    std::string(reinterpret_cast<const char *>(req_data.data()), req_data.size()));

	// Start DoPut stream
	std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
	std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;

	ARROW_ASSIGN_OR_RAISE(auto put_result, client_->DoPut(descriptor, batch->schema()));
	writer = std::move(put_result.writer);
	metadata_reader = std::move(put_result.reader);

	// Write batch
	ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
	ARROW_RETURN_NOT_OK(writer->DoneWriting());

	// Read response metadata
	std::shared_ptr<arrow::Buffer> metadata;
	ARROW_RETURN_NOT_OK(metadata_reader->ReadMetadata(&metadata));

	if (metadata) {
		auto resp_data = vector<uint8_t>(metadata->data(), metadata->data() + metadata->size());
		response = DistributedResponse::Deserialize(resp_data);
	} else {
		response.success = false;
		response.error_message = "No response from server";
	}

	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
                                                 std::unique_ptr<arrow::flight::FlightStreamReader> &stream) {

	DistributedRequest req;
	req.type = RequestType::SCAN_TABLE;
	req.table_name = table_name;
	req.limit = limit;
	req.offset = offset;

	// Serialize request to ticket
	auto req_data = req.Serialize();
	arrow::flight::Ticket ticket;
	ticket.ticket = std::string(reinterpret_cast<const char *>(req_data.data()), req_data.size());

	// Call DoGet and get the stream
	ARROW_ASSIGN_OR_RAISE(stream, client_->DoGet(ticket));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::SendAction(const DistributedRequest &req, DistributedResponse &resp) {

	// Serialize request
	auto req_data = req.Serialize();

	arrow::flight::Action action;
	action.type = "execute";
	action.body = arrow::Buffer::Wrap(req_data.data(), req_data.size());

	// Send action and get results
	std::unique_ptr<arrow::flight::ResultStream> results;
	ARROW_ASSIGN_OR_RAISE(results, client_->DoAction(action));

	// Read first result
	std::unique_ptr<arrow::flight::Result> result;
	ARROW_ASSIGN_OR_RAISE(result, results->Next());

	if (!result) {
		return arrow::Status::Invalid("No response from server");
	}

	// Deserialize response
	auto resp_data = vector<uint8_t>(result->body->data(), result->body->data() + result->body->size());
	resp = DistributedResponse::Deserialize(resp_data);

	return arrow::Status::OK();
}

} // namespace duckdb
