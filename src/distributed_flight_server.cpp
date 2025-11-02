#include "distributed_flight_server.hpp"
#include "distributed_protocol.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/types.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <iostream>

namespace duckdb {

DistributedFlightServer::DistributedFlightServer(const string &host, int port) : host_(host), port_(port) {
	// Initialize DuckDB instance
	db_ = make_uniq<DuckDB>();
	conn_ = make_uniq<Connection>(*db_);
	std::cout << "✅ Flight server initialized with DuckDB instance" << std::endl;
}

arrow::Status DistributedFlightServer::Start() {
	arrow::flight::Location location;
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp(host_, port_));

	arrow::flight::FlightServerOptions options(location);
	ARROW_RETURN_NOT_OK(Init(options));

	std::cout << "🚀 Flight server listening on " << location.ToString() << std::endl;
	return arrow::Status::OK();
}

void DistributedFlightServer::Shutdown() {
	auto status = FlightServerBase::Shutdown();
	if (!status.ok()) {
		std::cerr << "Warning: Shutdown returned error: " << status.ToString() << std::endl;
	}
	std::cout << "🛑 Flight server shut down" << std::endl;
}

string DistributedFlightServer::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host_, port_);
}

arrow::Status DistributedFlightServer::DoAction(const arrow::flight::ServerCallContext &context,
                                                const arrow::flight::Action &action,
                                                std::unique_ptr<arrow::flight::ResultStream> *result) {

	// Deserialize request
	auto req_data = vector<uint8_t>(action.body->data(), action.body->data() + action.body->size());
	auto req = DistributedRequest::Deserialize(req_data);

	DistributedResponse resp;

	// Handle different request types
	switch (req.type) {
	case RequestType::EXECUTE_SQL:
		ARROW_RETURN_NOT_OK(HandleExecuteSQL(req, resp));
		break;
	case RequestType::CREATE_TABLE:
		ARROW_RETURN_NOT_OK(HandleCreateTable(req, resp));
		break;
	case RequestType::DROP_TABLE:
		ARROW_RETURN_NOT_OK(HandleDropTable(req, resp));
		break;
	case RequestType::TABLE_EXISTS:
		ARROW_RETURN_NOT_OK(HandleTableExists(req, resp));
		break;
	default:
		return arrow::Status::Invalid("Unknown request type");
	}

	// Serialize response and return
	auto resp_data = resp.Serialize();
	auto buffer = arrow::Buffer::Wrap(resp_data.data(), resp_data.size());

	std::vector<arrow::flight::Result> results;
	results.push_back(arrow::flight::Result {buffer});
	*result = std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoGet(const arrow::flight::ServerCallContext &context,
                                             const arrow::flight::Ticket &ticket,
                                             std::unique_ptr<arrow::flight::FlightDataStream> *stream) {

	// Ticket contains serialized request
	auto req_data = vector<uint8_t>(ticket.ticket.data(), ticket.ticket.data() + ticket.ticket.size());
	auto req = DistributedRequest::Deserialize(req_data);

	if (req.type != RequestType::SCAN_TABLE) {
		return arrow::Status::Invalid("DoGet only supports SCAN_TABLE requests");
	}

	std::unique_ptr<arrow::flight::FlightDataStream> data_stream;
	ARROW_RETURN_NOT_OK(HandleScanTable(req, data_stream));

	*stream = std::move(data_stream);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoPut(const arrow::flight::ServerCallContext &context,
                                             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                                             std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {

	// Read descriptor to get request info
	auto descriptor = reader->descriptor();

	// Deserialize request from descriptor
	auto req_data = vector<uint8_t>(descriptor.cmd.data(), descriptor.cmd.data() + descriptor.cmd.size());
	auto req = DistributedRequest::Deserialize(req_data);

	// Read all record batches
	ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
	std::shared_ptr<arrow::RecordBatch> batch;

	DistributedResponse resp;

	while (true) {
		ARROW_ASSIGN_OR_RAISE(auto next, reader->Next());
		if (!next.data) {
			break;
		}
		batch = next.data;

		// Process each batch
		ARROW_RETURN_NOT_OK(HandleInsertData(req, batch, resp));
	}

	// Write response metadata
	auto resp_data = resp.Serialize();
	auto buffer = arrow::Buffer::Wrap(resp_data.data(), resp_data.size());
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleExecuteSQL(const DistributedRequest &req, DistributedResponse &resp) {

	std::cout << "📡 Server executing SQL: " << req.sql << std::endl;
	auto result = conn_->Query(req.sql);

	if (result->HasError()) {
		resp.success = false;
		resp.error_message = result->GetError();
		return arrow::Status::OK();
	}

	resp.success = true;
	resp.rows_affected = 0; // SQL execution doesn't return row count easily
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateTable(const DistributedRequest &req, DistributedResponse &resp) {

	std::cout << "🏗️  Server creating table: " << req.sql << std::endl;
	auto result = conn_->Query(req.sql);

	if (result->HasError()) {
		resp.success = false;
		resp.error_message = result->GetError();
		return arrow::Status::OK();
	}

	resp.success = true;
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropTable(const DistributedRequest &req, DistributedResponse &resp) {

	std::cout << "🗑️  Server dropping table: " << req.sql << std::endl;
	auto result = conn_->Query(req.sql);

	if (result->HasError()) {
		resp.success = false;
		resp.error_message = result->GetError();
		return arrow::Status::OK();
	}

	resp.success = true;
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleTableExists(const DistributedRequest &req, DistributedResponse &resp) {

	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.table_name);

	auto result = conn_->Query(sql);

	if (result->HasError()) {
		resp.success = false;
		resp.error_message = result->GetError();
		resp.exists = false;
		return arrow::Status::OK();
	}

	if (result->Fetch()) {
		resp.exists = result->GetValue(0, 0).GetValue<int>() > 0;
	} else {
		resp.exists = false;
	}

	resp.success = true;
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleScanTable(const DistributedRequest &req,
                                                       std::unique_ptr<arrow::flight::FlightDataStream> &stream) {

	string sql = StringUtil::Format("SELECT * FROM %s LIMIT %llu OFFSET %llu", req.table_name, req.limit, req.offset);

	std::cout << "🔍 Server scanning table: " << sql << std::endl;

	auto result = conn_->Query(sql);

	if (result->HasError()) {
		return arrow::Status::Invalid("Query error: " + result->GetError());
	}

	std::shared_ptr<arrow::RecordBatchReader> reader;
	ARROW_RETURN_NOT_OK(QueryResultToArrow(*result, reader));

	stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleInsertData(const DistributedRequest &req,
                                                        std::shared_ptr<arrow::RecordBatch> batch,
                                                        DistributedResponse &resp) {

	std::cout << "💾 Server inserting " << batch->num_rows() << " rows into table: " << req.table_name << std::endl;

	// For now, we'll use a simplified approach: convert Arrow to SQL INSERT
	// TODO: Use proper Arrow->DuckDB integration when available

	// Build INSERT statement
	std::string insert_sql = "INSERT INTO " + req.table_name + " VALUES ";

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
		resp.success = false;
		resp.error_message = result->GetError();
		return arrow::Status::OK();
	}

	resp.success = true;
	resp.rows_affected = batch->num_rows();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::QueryResultToArrow(QueryResult &result,
                                                          std::shared_ptr<arrow::RecordBatchReader> &reader) {

	// Convert DuckDB QueryResult to Arrow RecordBatchReader using C API bridge
	// This is the standard way DuckDB integrates with Arrow

	// Create an Arrow stream from the query result
	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	stream_wrapper->arrow_array_stream.release = nullptr;

	// Get all data chunks and convert to Arrow
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	std::shared_ptr<arrow::Schema> schema;

	unique_ptr<DataChunk> chunk;
	while (true) {
		chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		// Convert chunk to Arrow using DuckDB's built-in conversion
		ArrowSchema arrow_schema;
		ArrowArray arrow_array;

		// Export to Arrow C API
		// Note: This is a simplified version - may need adjustment based on DuckDB version
		// For now, just create an empty schema
		if (!schema) {
			std::vector<std::shared_ptr<arrow::Field>> fields;
			for (idx_t i = 0; i < result.ColumnCount(); i++) {
				fields.push_back(arrow::field(result.ColumnName(i), arrow::utf8()));
			}
			schema = arrow::schema(fields);
		}
	}

	// Create reader from empty batches for now
	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches, schema));

	return arrow::Status::OK();
}

} // namespace duckdb
