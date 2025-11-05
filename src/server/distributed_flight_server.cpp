#include "distributed_flight_server.hpp"

#include "duckling_storage.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/types.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <iostream>

namespace duckdb {

DistributedFlightServer::DistributedFlightServer(string host_p, int port_p) : host(std::move(host_p)), port(port_p) {
	std::cerr << "\n========================================" << std::endl;
	std::cerr << "[SERVER INIT] Starting server initialization" << std::endl;
	std::cerr << "========================================" << std::endl;

	// Register the Duckling storage extension
	// For now, we just register it without attaching as a separate database
	// The extension is available for future use (monitoring, fleet distribution, etc.)
	// but operates transparently as a no-op wrapper around standard DuckDB
	std::cerr << "[SERVER INIT] Registering Duckling storage extension..." << std::endl;
	DBConfig config;
	config.storage_extensions["duckling"] = make_uniq<DucklingStorageExtension>();

	db = make_uniq<DuckDB>(nullptr, &config);
	conn = make_uniq<Connection>(*db);
	
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, "Duckling storage extension registered (no-op mode for now)");
	std::cerr << "[SERVER INIT] DuckDB instance created with Duckling extension registered" << std::endl;

	std::cerr << "========================================" << std::endl;
	std::cerr << "[SERVER INIT] Initialization complete\n" << std::endl;
}

arrow::Status DistributedFlightServer::Start() {
	arrow::flight::Location location;
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp(host, port));

	arrow::flight::FlightServerOptions options(location);
	ARROW_RETURN_NOT_OK(Init(options));

	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Server started on %s:%d", host, port));

	return arrow::Status::OK();
}

void DistributedFlightServer::Shutdown() {
	auto status = FlightServerBase::Shutdown();
	// Ignore shutdown errors in production
}

string DistributedFlightServer::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host, port);
}

arrow::Status DistributedFlightServer::DoAction(const arrow::flight::ServerCallContext &context,
                                                const arrow::flight::Action &action,
                                                std::unique_ptr<arrow::flight::ResultStream> *result) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(action.body->data(), action.body->size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	distributed::DistributedResponse response;
	response.set_success(true);

	switch (request.request_case()) {
	// ========== Table perations ==========
	case distributed::DistributedRequest::kCreateTable:
		ARROW_RETURN_NOT_OK(HandleCreateTable(request.create_table(), response));
		break;
	case distributed::DistributedRequest::kDropTable:
		ARROW_RETURN_NOT_OK(HandleDropTable(request.drop_table(), response));
		break;
	case distributed::DistributedRequest::kAlterTable:
		ARROW_RETURN_NOT_OK(HandleAlterTable(request.alter_table(), response));
		break;

	// ========== Index perations ==========
	case distributed::DistributedRequest::kCreateIndex:
		ARROW_RETURN_NOT_OK(HandleCreateIndex(request.create_index(), response));
		break;
	case distributed::DistributedRequest::kDropIndex:
		ARROW_RETURN_NOT_OK(HandleDropIndex(request.drop_index(), response));
		break;

	// ========== Query & Utility Operations ==========
	case distributed::DistributedRequest::kExecuteSql:
		ARROW_RETURN_NOT_OK(HandleExecuteSQL(request.execute_sql(), response));
		break;
	case distributed::DistributedRequest::kTableExists:
		ARROW_RETURN_NOT_OK(HandleTableExists(request.table_exists(), response));
		break;
	case distributed::DistributedRequest::kLoadExtension:
		ARROW_RETURN_NOT_OK(HandleLoadExtension(request.load_extension(), response));
		break;

	// ========== Error Cases ==========
	case distributed::DistributedRequest::REQUEST_NOT_SET:
		return arrow::Status::Invalid("Request type not set");
	default:
		return arrow::Status::Invalid("Unknown request type");
	}

	std::string response_data = response.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(response_data);

	std::vector<arrow::flight::Result> results;
	results.emplace_back(arrow::flight::Result {buffer});
	*result = std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoGet(const arrow::flight::ServerCallContext &context,
                                             const arrow::flight::Ticket &ticket,
                                             std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(ticket.ticket.data(), ticket.ticket.size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

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
	auto descriptor = reader->descriptor();
	std::string table_name;
	if (!descriptor.path.empty()) {
		table_name = descriptor.path[0];
	}

	std::cerr << "[SERVER] DoPut called for table: " << table_name << std::endl;

	// Read all record batches.
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

		std::cerr << "[SERVER] DoPut: Processing batch for table: " << table_name << std::endl;
		// Process each batch
		ARROW_RETURN_NOT_OK(HandleInsertData(table_name, batch, resp));
	}

	// Write response metadata.
	std::string resp_data = resp.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(resp_data);
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleExecuteSQL(const distributed::ExecuteSQLRequest &req,
                                                        distributed::DistributedResponse &resp) {
	std::cerr << "[SERVER] HandleExecuteSQL: " << req.sql() << std::endl;
	auto result = conn->Query(req.sql());

	if (result->HasError()) {
		std::cerr << "[SERVER] HandleExecuteSQL ERROR: " << result->GetError() << std::endl;
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	std::cerr << "[SERVER] HandleExecuteSQL SUCCESS" << std::endl;
	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_sql();
	exec_resp->set_rows_affected(0);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateTable(const distributed::CreateTableRequest &req,
                                                         distributed::DistributedResponse &resp) {
	auto &db_instance = *db->instance;
	std::cerr << "[SERVER] HandleCreateTable: " << req.sql() << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("HandleCreateTable: %s", req.sql()));
	
	auto result = conn->Query(req.sql());

	if (result->HasError()) {
		std::cerr << "[SERVER] HandleCreateTable ERROR: " << result->GetError() << std::endl;
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	std::cerr << "[SERVER] HandleCreateTable SUCCESS" << std::endl;
	resp.set_success(true);
	resp.mutable_create_table();

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropTable(const distributed::DropTableRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP TABLE IF EXISTS " + req.table_name();
	std::cerr << "[SERVER] HandleDropTable: " << sql << std::endl;
	auto result = conn->Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_drop_table();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateIndex(const distributed::CreateIndexRequest &req,
                                                         distributed::DistributedResponse &resp) {
	auto &db_instance = *db->instance;
	std::cerr << "[SERVER] HandleCreateIndex: " << req.sql() << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("HandleCreateIndex: %s", req.sql()));
	
	auto result = conn->Query(req.sql());

	if (result->HasError()) {
		std::cerr << "[SERVER] HandleCreateIndex ERROR: " << result->GetError() << std::endl;
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	std::cerr << "[SERVER] HandleCreateIndex SUCCESS" << std::endl;
	resp.set_success(true);
	resp.mutable_create_index();

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropIndex(const distributed::DropIndexRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP INDEX IF EXISTS " + req.index_name();
	std::cerr << "[SERVER] HandleDropIndex: " << sql << std::endl;
	auto result = conn->Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_drop_index();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleAlterTable(const distributed::AlterTableRequest &req,
                                                        distributed::DistributedResponse &resp) {
	auto result = conn->Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_alter_table();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleLoadExtension(const distributed::LoadExtensionRequest &req,
                                                           distributed::DistributedResponse &resp) {
	auto &db_instance = *db->instance;
	string sql = "LOAD " + req.extension_name();

	// Handle INSTALL with repository and version.
	if (!req.repository().empty() || !req.version().empty()) {
		sql = "INSTALL " + req.extension_name();
		if (!req.repository().empty()) {
			sql += " FROM '" + req.repository() + "'";
		}
		if (!req.version().empty()) {
			sql += " VERSION '" + req.version() + "'";
		}
	}

	// Execute INSTALL first.
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Install extension with %s", sql));
	auto install_result = conn->Query(sql);
	if (install_result->HasError()) {
		const string error_message =
		    StringUtil::Format("Extension %s install failed %s", req.extension_name(), install_result->GetError());
		DUCKDB_LOG_DEBUG(db_instance, error_message);
		resp.set_success(false);
		resp.set_error_message(std::move(error_message));
		return arrow::Status::OK();
	}

	// Then LOAD the extension.
	sql = "LOAD " + req.extension_name();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Load extension with %s", sql));
	auto load_result = conn->Query(sql);
	if (load_result->HasError()) {
		const string error_message =
		    StringUtil::Format("Extension %s load failed %s", req.extension_name(), load_result->GetError());
		DUCKDB_LOG_DEBUG(db_instance, error_message);
		resp.set_success(false);
		resp.set_error_message(std::move(error_message));
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_load_extension();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleTableExists(const distributed::TableExistsRequest &req,
                                                         distributed::DistributedResponse &resp) {
	string sql = StringUtil::Format(
	    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'",
	    req.table_name());
	std::cerr << "[SERVER] HandleTableExists: Checking for table " << req.table_name() << std::endl;

	auto result = conn->Query(sql);

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
	string sql = StringUtil::Format("SELECT * FROM %s LIMIT %llu OFFSET %llu", req.table_name(), 
	                                req.limit(), req.offset());
	std::cerr << "[SERVER] HandleScanTable: " << sql << std::endl;
	auto result = conn->Query(sql);

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
	// TODO(hjiang): Current implementation is pretty insufficient, which directly executes insertion statement.
	// Better to call native duckdb APIs for ingestion.

	std::cerr << "[SERVER] HandleInsertData: Inserting into " << table_name << std::endl;

	// Build INSERT statement.
	std::string insert_sql = "INSERT INTO " + table_name + " VALUES ";

	for (int64_t row = 0; row < batch->num_rows(); row++) {
		if (row > 0) {
			insert_sql += ", ";
		}
		insert_sql += "(";

		for (int col = 0; col < batch->num_columns(); col++) {
			if (col > 0) {
				insert_sql += ", ";
			}

			auto array = batch->column(col);
			// Simple value extraction - handle NULL and basic types
			if (array->IsNull(row)) {
				insert_sql += "NULL";
			} else {
				insert_sql += "'" + array->ToString() + "'";
			}
		}
		insert_sql += ")";
	}

	std::cerr << "[SERVER] HandleInsertData: Executing SQL: " << insert_sql << std::endl;
	auto result = conn->Query(insert_sql);
	if (result->HasError()) {
		std::cerr << "[SERVER] HandleInsertData ERROR: " << result->GetError() << std::endl;
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	std::cerr << "[SERVER] HandleInsertData SUCCESS" << std::endl;
	resp.set_success(true);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::QueryResultToArrow(QueryResult &result,
                                                          std::shared_ptr<arrow::RecordBatchReader> &reader) {
	// Create Arrow schema from DuckDB types.
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, result.client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));

	// Collect all data chunks and convert to Arrow RecordBatches.
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		// Convert DataChunk to Arrow using C API.
		ArrowArray arrow_array;
		auto extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(*result.client_properties.client_context, result.types);
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, result.client_properties, extension_types);

		// Import to Arrow C++ RecordBatch.
		auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
		if (!batch_result.ok()) {
			return arrow::Status::Invalid("Failed to import Arrow batch: " + batch_result.status().ToString());
		}

		// TODO(hjiang): Avoid exception thrown.
		batches.emplace_back(batch_result.ValueOrDie());
	}

	// Create RecordBatchReader from collected batches.
	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches, schema));

	return arrow::Status::OK();
}

} // namespace duckdb
