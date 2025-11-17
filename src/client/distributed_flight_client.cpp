#include "distributed_flight_client.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"

#include <arrow/buffer.h>

namespace duckdb {

DistributedFlightClient::DistributedFlightClient(string server_url_p) : server_url(std::move(server_url_p)) {
}

arrow::Status DistributedFlightClient::Connect() {
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::Parse(server_url));
	ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));
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
	distributed::DistributedRequest req;
	auto *create_req = req.mutable_create_table();
	create_req->set_sql(create_sql);
	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::DropTable(const string &drop_sql, distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	auto *drop_req = req.mutable_drop_table();
	drop_req->set_table_name(drop_sql);
	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::CreateIndex(const string &create_sql,
                                                   distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	auto *create_req = req.mutable_create_index();
	create_req->set_sql(create_sql);
	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::DropIndex(const string &index_name, distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	auto *drop_req = req.mutable_drop_index();
	drop_req->set_index_name(index_name);
	return SendAction(req, response);
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
	arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({table_name});

	std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
	std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;

	ARROW_ASSIGN_OR_RAISE(auto put_result, client->DoPut(descriptor, batch->schema()));
	writer = std::move(put_result.writer);
	metadata_reader = std::move(put_result.reader);

	// Write batch.
	ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
	ARROW_RETURN_NOT_OK(writer->DoneWriting());

	// Read response metadata.
	std::shared_ptr<arrow::Buffer> metadata;
	ARROW_RETURN_NOT_OK(metadata_reader->ReadMetadata(&metadata));

	if (metadata != nullptr) {
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
	distributed::DistributedRequest req;
	auto *scan_req = req.mutable_scan_table();
	scan_req->set_table_name(table_name);
	scan_req->set_limit(limit);
	scan_req->set_offset(offset);

	std::string req_data = req.SerializeAsString();
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;
	ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(ticket));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightClient::GetQueryExecutionStats(distributed::DistributedResponse &response) {
	distributed::DistributedRequest req;
	req.mutable_get_query_execution_stats();
	return SendAction(req, response);
}

arrow::Status DistributedFlightClient::SendAction(const distributed::DistributedRequest &req,
                                                  distributed::DistributedResponse &resp) {
	std::string req_data = req.SerializeAsString();

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
