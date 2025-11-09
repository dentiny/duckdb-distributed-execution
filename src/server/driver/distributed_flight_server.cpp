#include "server/driver/distributed_flight_server.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "query_common.hpp"
#include "server/driver/duckling_storage.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

DistributedFlightServer::DistributedFlightServer(string host_p, int port_p) : host(std::move(host_p)), port(port_p) {
	// Register the Duckling storage extension.
	DBConfig config;
	config.storage_extensions["duckling"] = make_uniq<DucklingStorageExtension>();

	db = make_uniq<DuckDB>(nullptr, &config);
	conn = make_uniq<Connection>(*db);
	auto &db_instance = *db->instance.get();

	// Attach duckling storage extension.
	DUCKDB_LOG_DEBUG(db_instance, "Attaching Duckling storage extension");
	auto result = conn->Query("ATTACH DATABASE ':memory:' AS duckling (TYPE duckling);");
	if (result->HasError()) {
		auto error_message = StringUtil::Format("Failed to attach Duckling: %s", result->GetError());
		DUCKDB_LOG_DEBUG(db_instance, error_message);
		throw InternalException(error_message);
	}

	// Set duckling as the default database.
	auto use_result = conn->Query("USE duckling;");
	if (use_result->HasError()) {
		auto error_message = StringUtil::Format("Failed to USE duckling: %s", use_result->GetError());
		DUCKDB_LOG_DEBUG(db_instance, error_message);
		throw InternalException(error_message);
	}
	DUCKDB_LOG_DEBUG(db_instance, "Duckling attach and set as default catalog");

	// Initialize worker manager and distributed executor
	worker_manager = make_uniq<WorkerManager>(*db);
	distributed_executor = make_uniq<DistributedExecutor>(*worker_manager, *conn);
}

DatabaseInstance &DistributedFlightServer::GetDatabaseInstance() {
	return *db->instance;
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

arrow::Status DistributedFlightServer::StartWithWorkers(idx_t num_workers) {
	auto &db_instance = *db->instance.get();

	// Start local workers.
	if (num_workers > 0) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Starting %llu local workers", num_workers));
		try {
			worker_manager->StartLocalWorkers(num_workers);
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Started %llu workers", num_workers));
		} catch (std::exception &e) {
			return arrow::Status::IOError("Failed to start workers: " + string(e.what()));
		}
	}

	// Start the server.
	return Start();
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
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, "DistributedFlightServer::DoGet called");

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
	// Try distributed execution first if workers are available.
	unique_ptr<QueryResult> result;
	if (worker_manager != nullptr && worker_manager->GetWorkerCount() > 0) {
		result = distributed_executor->ExecuteDistributed(req.sql());
	}

	// Fall back to local execution if not distributed.
	if (result == nullptr) {
		result = conn->Query(req.sql());
	}

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
	auto &db_instance = *db->instance;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("HandleCreateTable: %s", req.sql()));

	auto result = conn->Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_create_table();

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropTable(const distributed::DropTableRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP TABLE IF EXISTS " + req.table_name();
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
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("HandleCreateIndex: %s", req.sql()));

	auto result = conn->Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_create_index();

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropIndex(const distributed::DropIndexRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP INDEX IF EXISTS " + req.index_name();
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

	// Execute INSTALL first.
	string sql = "INSTALL " + req.extension_name();
	if (!req.repository().empty() || !req.version().empty()) {
		if (!req.repository().empty()) {
			sql += " FROM '" + req.repository() + "'";
		}
		if (!req.version().empty()) {
			sql += " VERSION '" + req.version() + "'";
		}
	}
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
	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.table_name());

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
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Handling scan for table: %s", req.table_name()));

	// AGGREGATION PUSHDOWN FIX:
	// Check if table_name actually contains full SQL (temp hack for testing)
	// In the future, this should come from a dedicated field in the protocol
	string sql;
	string table_identifier = req.table_name();

	// If it looks like SQL (contains SELECT), use it as-is
	// Otherwise, generate SELECT * FROM table
	if (StringUtil::Contains(StringUtil::Upper(table_identifier), "SELECT")) {
		sql = table_identifier;
		DUCKDB_LOG_DEBUG(db_instance, "[AGGREGATION PUSHDOWN] Using custom SQL from request");
	} else {
		sql = StringUtil::Format("SELECT * FROM %s", table_identifier);
	}

	if (req.limit() != NO_QUERY_LIMIT && req.limit() != STANDARD_VECTOR_SIZE) {
		sql += StringUtil::Format(" LIMIT %llu ", req.limit());
	}
	if (req.offset() != NO_QUERY_OFFSET) {
		sql += StringUtil::Format(" OFFSET %llu ", req.offset());
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("[AGGREGATION PUSHDOWN] Final SQL: %s", sql));

	// Try distributed execution first if workers are available.
	unique_ptr<QueryResult> result;
	if (worker_manager != nullptr && worker_manager->GetWorkerCount() > 0) {
		result = distributed_executor->ExecuteDistributed(sql);
	}

	// Fall back to local execution if not distributed.
	if (result == nullptr) {
		result = conn->Query(sql);
	}

	if (result->HasError()) {
		return arrow::Status::Invalid("Query error: " + result->GetError());
	}

	if (!result->client_properties.client_context) {
		result->client_properties.client_context = conn->context.get();
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

	auto result = conn->Query(insert_sql);
	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::QueryResultToArrow(QueryResult &result,
                                                          std::shared_ptr<arrow::RecordBatchReader> &reader,
                                                          idx_t *row_count) {
	// Create Arrow schema from DuckDB types.
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, result.client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));

	// Collect all data chunks and convert to Arrow RecordBatches.
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	idx_t count = 0;

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
		auto batch = batch_result.ValueOrDie();
		count += batch->num_rows();
		batches.emplace_back(batch);
	}

	// Create RecordBatchReader from collected batches.
	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches, schema));
	if (row_count) {
		*row_count = count;
	}

	return arrow::Status::OK();
}

} // namespace duckdb
