#include "distributed_flight_server.hpp"

#include "distributed_protocol.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/types.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

DistributedFlightServer::DistributedFlightServer(const string &host, int port) : host_(host), port_(port) {
	// Initialize DuckDB instance
	db_ = make_uniq<DuckDB>();
	conn_ = make_uniq<Connection>(*db_);
}

arrow::Status DistributedFlightServer::Start() {
	arrow::flight::Location location;
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp(host_, port_));

	arrow::flight::FlightServerOptions options(location);
	ARROW_RETURN_NOT_OK(Init(options));

	return arrow::Status::OK();
}

void DistributedFlightServer::Shutdown() {
	auto status = FlightServerBase::Shutdown();
	// Ignore shutdown errors in production
}

string DistributedFlightServer::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host_, port_);
}

arrow::Status DistributedFlightServer::DoAction(const arrow::flight::ServerCallContext &context,
                                                const arrow::flight::Action &action,
                                                std::unique_ptr<arrow::flight::ResultStream> *result) {

	// Deserialize protobuf request directly
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(action.body->data(), action.body->size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	// Create response
	distributed::DistributedResponse response;
	response.set_success(true);

	// Handle different request types using oneof
	switch (request.request_case()) {
	case distributed::DistributedRequest::kExecuteSql:
		ARROW_RETURN_NOT_OK(HandleExecuteSQL(request.execute_sql(), response));
		break;
	case distributed::DistributedRequest::kCreateTable:
		ARROW_RETURN_NOT_OK(HandleCreateTable(request.create_table(), response));
		break;
	case distributed::DistributedRequest::kDropTable:
		ARROW_RETURN_NOT_OK(HandleDropTable(request.drop_table(), response));
		break;
	case distributed::DistributedRequest::kTableExists:
		ARROW_RETURN_NOT_OK(HandleTableExists(request.table_exists(), response));
		break;
	case distributed::DistributedRequest::REQUEST_NOT_SET:
		return arrow::Status::Invalid("Request type not set");
	default:
		return arrow::Status::Invalid("Unknown request type");
	}

	// Serialize response
	std::string response_data = response.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(response_data);

	std::vector<arrow::flight::Result> results;
	results.push_back(arrow::flight::Result {buffer});
	*result = std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoGet(const arrow::flight::ServerCallContext &context,
                                             const arrow::flight::Ticket &ticket,
                                             std::unique_ptr<arrow::flight::FlightDataStream> *stream) {

	// Deserialize protobuf request
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(ticket.ticket.data(), ticket.ticket.size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	// DoGet only handles scan requests
	if (request.request_case() != distributed::DistributedRequest::kScanTable) {
		return arrow::Status::Invalid("DoGet only supports SCAN_TABLE requests");
	}

	std::unique_ptr<arrow::flight::FlightDataStream> data_stream;
	ARROW_RETURN_NOT_OK(HandleScanTable(request.scan_table(), data_stream));

	*stream = std::move(data_stream);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoPut(const arrow::flight::ServerCallContext &context,
                                             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                                             std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {

	// Read descriptor to get table name from descriptor path
	auto descriptor = reader->descriptor();
	std::string table_name;
	if (!descriptor.path.empty()) {
		table_name = descriptor.path[0];
	}

	// Read all record batches
	ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
	std::shared_ptr<arrow::RecordBatch> batch;

	distributed::DistributedResponse resp;
	resp.set_success(true);

	while (true) {
		ARROW_ASSIGN_OR_RAISE(auto next, reader->Next());
		if (!next.data) {
			break;
		}
		batch = next.data;

		// Process each batch
		ARROW_RETURN_NOT_OK(HandleInsertData(table_name, batch, resp));
	}

	// Write response metadata
	std::string resp_data = resp.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(resp_data);
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleExecuteSQL(const distributed::ExecuteSQLRequest &req,
                                                        distributed::DistributedResponse &resp) {

	std::cout << "📡 Server executing SQL: " << req.sql() << std::endl;
	auto result = conn_->Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_sql();
	exec_resp->set_rows_affected(0);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateTable(const distributed::CreateTableRequest &req,
                                                         distributed::DistributedResponse &resp) {
	// SERVER SIDE: Received CreateTableRequest protobuf message
	auto result = conn_->Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	// Build response using protobuf oneof
	resp.set_success(true);
	resp.mutable_create_table(); // Sets oneof to CreateTableResponse

	// This returns:
	// - resp.SerializeAsString() → bytes
	// - Arrow Flight returns bytes to client
	// - Client does: response.ParseFromArray(bytes)
	// - Client checks: response.success() and response.has_create_table()

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropTable(const distributed::DropTableRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP TABLE IF EXISTS " + req.table_name();
	auto result = conn_->Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_drop_table();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleTableExists(const distributed::TableExistsRequest &req,
                                                         distributed::DistributedResponse &resp) {
	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.table_name());

	auto result = conn_->Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	auto *exists_resp = resp.mutable_table_exists();
	if (result->Fetch()) {
		exists_resp->set_exists(result->GetValue(0, 0).GetValue<int>() > 0);
	} else {
		exists_resp->set_exists(false);
	}

	resp.set_success(true);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleScanTable(const distributed::ScanTableRequest &req,
                                                       std::unique_ptr<arrow::flight::FlightDataStream> &stream) {

	string sql =
	    StringUtil::Format("SELECT * FROM %s LIMIT %llu OFFSET %llu", req.table_name(), req.limit(), req.offset());
	auto result = conn_->Query(sql);

	if (result->HasError()) {
		return arrow::Status::Invalid("Query error: " + result->GetError());
	}

	std::shared_ptr<arrow::RecordBatchReader> reader;
	ARROW_RETURN_NOT_OK(QueryResultToArrow(*result, reader));

	stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleInsertData(const std::string &table_name,
                                                        std::shared_ptr<arrow::RecordBatch> batch,
                                                        distributed::DistributedResponse &resp) {
	// For now, we'll use a simplified approach: convert Arrow to SQL INSERT
	// TODO: Use proper Arrow->DuckDB integration when available

	// Build INSERT statement
	std::string insert_sql = "INSERT INTO " + table_name + " VALUES ";

	for (int64_t row = 0; row < batch->num_rows(); row++) {
		if (row > 0)
			insert_sql += ", ";
		insert_sql += "(";

		for (int col = 0; col < batch->num_columns(); col++) {
			if (col > 0)
				insert_sql += ", ";

			auto array = batch->column(col);
			// Simple value extraction - handle NULL and basic types
			if (array->IsNull(row)) {
				insert_sql += "NULL";
			} else {
				// TODO: Proper type handling for all Arrow types
				// For now, convert to string representation
				insert_sql += "'" + array->ToString() + "'";
			}
		}
		insert_sql += ")";
	}

	auto result = conn_->Query(insert_sql);
	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::QueryResultToArrow(QueryResult &result,
                                                          std::shared_ptr<arrow::RecordBatchReader> &reader) {

	// Convert DuckDB QueryResult to Arrow using C API bridge

	// Step 1: Create Arrow schema from DuckDB types
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, result.client_properties);

	// Convert to Arrow C++ schema
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));

	// Step 2: Collect all data chunks and convert to Arrow RecordBatches
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		// Convert DataChunk to Arrow using C API
		ArrowArray arrow_array;
		auto extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(*result.client_properties.client_context, result.types);
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, result.client_properties, extension_types);

		// Import to Arrow C++ RecordBatch
		auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
		if (!batch_result.ok()) {
			return arrow::Status::Invalid("Failed to import Arrow batch: " + batch_result.status().ToString());
		}

		batches.push_back(batch_result.ValueOrDie());
	}

	// Step 3: Create RecordBatchReader from collected batches
	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches, schema));

	return arrow::Status::OK();
}

} // namespace duckdb
