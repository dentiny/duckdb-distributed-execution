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

arrow::Status DistributedFlightClient::ExecuteSQL(const string &sql, distributed::DistributedResponse &response) {

	distributed::DistributedRequest req;
	auto *exec_req = req.mutable_execute_sql();
	exec_req->set_sql(sql);

	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::CreateTable(const string &create_sql,
                                                   distributed::DistributedResponse &response) {
	// CLIENT SIDE: Build protobuf request with oneof
	distributed::DistributedRequest req;
	auto *create_req = req.mutable_create_table(); // Sets oneof to CreateTableRequest
	create_req->set_sql(create_sql);

	// This sends:
	// - req.SerializeAsString() → bytes
	// - Arrow Flight DoAction(bytes)
	//
	// Server receives and does:
	// - req.ParseFromArray(bytes)
	// - switch(req.request_case()) case kCreateTable:
	// - HandleCreateTable(req.create_table())  // Type-safe protobuf access

	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::DropTable(const string &drop_sql, distributed::DistributedResponse &response) {

	distributed::DistributedRequest req;
	auto *drop_req = req.mutable_drop_table();
	drop_req->set_table_name(drop_sql);

	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::TableExists(const string &table_name, bool &exists) {

	distributed::DistributedRequest req;
	auto *exists_req = req.mutable_table_exists();
	exists_req->set_table_name(table_name);

	distributed::DistributedResponse resp;
	ARROW_RETURN_NOT_OK(SendAction(req, resp));

	if (!resp.success()) {
		return arrow::Status::Invalid(resp.error_message());
	}

	exists = resp.table_exists().exists();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::InsertData(const string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
                                                  distributed::DistributedResponse &response) {

	// Use descriptor path to pass table name
	arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({table_name});

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
		if (!response.ParseFromArray(metadata->data(), metadata->size())) {
			return arrow::Status::Invalid("Failed to parse response");
		}
	} else {
		response.set_success(false);
		response.set_error_message("No response from server");
	}

	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
                                                 std::unique_ptr<arrow::flight::FlightStreamReader> &stream) {
	// CLIENT SIDE: Build protobuf request for table scan
	distributed::DistributedRequest req;
	auto *scan_req = req.mutable_scan_table(); // Sets oneof to ScanTableRequest
	scan_req->set_table_name(table_name);
	scan_req->set_limit(limit);
	scan_req->set_offset(offset);

	// Serialize protobuf to bytes and create Flight ticket
	std::string req_data = req.SerializeAsString();
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;

	// Send via Arrow Flight DoGet
	// Server will:
	// 1. req.ParseFromArray(ticket bytes)
	// 2. switch(req.request_case()) case kScanTable
	// 3. HandleScanTable(req.scan_table()) - gets table_name, limit, offset
	// 4. Execute query and return Arrow RecordBatches (NOT protobuf - efficient!)

	ARROW_ASSIGN_OR_RAISE(stream, client_->DoGet(ticket));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::SendAction(const distributed::DistributedRequest &req,
                                                  distributed::DistributedResponse &resp) {

	// Serialize request
	std::string req_data = req.SerializeAsString();

	arrow::flight::Action action;
	action.type = "execute";
	action.body = arrow::Buffer::FromString(req_data);

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
	if (!resp.ParseFromArray(result->body->data(), result->body->size())) {
		return arrow::Status::Invalid("Failed to parse response");
	}

	return arrow::Status::OK();
}

} // namespace duckdb
