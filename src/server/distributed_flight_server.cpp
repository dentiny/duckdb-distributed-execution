#include "distributed_flight_server.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/types.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

DistributedFlightServer::DistributedFlightServer(string host_p, int port_p) : host(std::move(host_p)), port(port_p) {
}

arrow::Status DistributedFlightServer::Start() {
	arrow::flight::Location location;
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp(host, port));

	arrow::flight::FlightServerOptions options(location);
	ARROW_RETURN_NOT_OK(Init(options));

	return arrow::Status::OK();
}

void DistributedFlightServer::Shutdown() {
	auto status = FlightServerBase::Shutdown();
	// Ignore shutdown errors in production
}

string DistributedFlightServer::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host, port);
}

Connection &DistributedFlightServer::GetConnection(const string &db_path) {
	std::lock_guard<std::mutex> lock(connections_mutex);

	auto it = connections.find(db_path);
	if (it != connections.end()) {
		return *it->second->conn;
	}

	// Create new database connection.
	auto db_conn = make_uniq<DatabaseConnection>();
	if (db_path.empty()) {
		// In-memory database.
		db_conn->db = make_uniq<DuckDB>();
	} else {
		// File-based database.
		db_conn->db = make_uniq<DuckDB>(db_path);
	}
	db_conn->conn = make_uniq<Connection>(*db_conn->db);

	auto *conn_ptr = db_conn->conn.get();
	connections[db_path] = std::move(db_conn);
	return *conn_ptr;
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

	const string &db_path = request.db_path();

	switch (request.request_case()) {
	case distributed::DistributedRequest::kExecuteSql:
		ARROW_RETURN_NOT_OK(HandleExecuteSQL(db_path, request.execute_sql(), response));
		break;
	case distributed::DistributedRequest::kCreateTable:
		ARROW_RETURN_NOT_OK(HandleCreateTable(db_path, request.create_table(), response));
		break;
	case distributed::DistributedRequest::kDropTable:
		ARROW_RETURN_NOT_OK(HandleDropTable(db_path, request.drop_table(), response));
		break;
	case distributed::DistributedRequest::kCreateIndex:
		ARROW_RETURN_NOT_OK(HandleCreateIndex(db_path, request.create_index(), response));
		break;
	case distributed::DistributedRequest::kDropIndex:
		ARROW_RETURN_NOT_OK(HandleDropIndex(db_path, request.drop_index(), response));
		break;
	case distributed::DistributedRequest::kAlterTable:
		ARROW_RETURN_NOT_OK(HandleAlterTable(db_path, request.alter_table(), response));
		break;
	case distributed::DistributedRequest::kTableExists:
		ARROW_RETURN_NOT_OK(HandleTableExists(db_path, request.table_exists(), response));
		break;
	case distributed::DistributedRequest::kGetCatalogInfo:
		ARROW_RETURN_NOT_OK(HandleGetCatalogInfo(db_path, request.get_catalog_info(), response));
		break;
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

	const string &db_path = request.db_path();
	std::unique_ptr<arrow::flight::FlightDataStream> data_stream;
	ARROW_RETURN_NOT_OK(HandleScanTable(db_path, request.scan_table(), data_stream));

	*stream = std::move(data_stream);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoPut(const arrow::flight::ServerCallContext &context,
                                             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                                             std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {
	auto descriptor = reader->descriptor();

	// Extract db_path and table_name from the FlightDescriptor path
	// Path format: [db_path, table_name]
	string db_path = "";
	std::string table_name;
	if (descriptor.path.size() >= 2) {
		db_path = descriptor.path[0];
		table_name = descriptor.path[1];
	} else if (descriptor.path.size() == 1) {
		// Fallback for backward compatibility: just table_name, empty db_path
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

		// Process each batch
		ARROW_RETURN_NOT_OK(HandleInsertData(db_path, table_name, batch, resp));
	}

	// Write response metadata.
	std::string resp_data = resp.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(resp_data);
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleExecuteSQL(const string &db_path,
                                                        const distributed::ExecuteSQLRequest &req,
                                                        distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto result = conn.Query(req.sql());

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

arrow::Status DistributedFlightServer::HandleCreateTable(const string &db_path,
                                                         const distributed::CreateTableRequest &req,
                                                         distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto result = conn.Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_create_table();

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropTable(const string &db_path, const distributed::DropTableRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto sql = "DROP TABLE IF EXISTS " + req.table_name();
	auto result = conn.Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_drop_table();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateIndex(const string &db_path,
                                                         const distributed::CreateIndexRequest &req,
                                                         distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto result = conn.Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_create_index();

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleDropIndex(const string &db_path, const distributed::DropIndexRequest &req,
                                                       distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto sql = "DROP INDEX IF EXISTS " + req.index_name();
	auto result = conn.Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_drop_index();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleAlterTable(const string &db_path,
                                                        const distributed::AlterTableRequest &req,
                                                        distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto result = conn.Query(req.sql());

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_alter_table();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleTableExists(const string &db_path,
                                                         const distributed::TableExistsRequest &req,
                                                         distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.table_name());

	auto result = conn.Query(sql);

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

arrow::Status DistributedFlightServer::HandleGetCatalogInfo(const string &db_path,
                                                            const distributed::GetCatalogInfoRequest &req,
                                                            distributed::DistributedResponse &resp) {
	auto &conn = GetConnection(db_path);
	auto *catalog_resp = resp.mutable_get_catalog_info();

	// Query all tables and their columns
	string tables_sql = "SELECT table_schema, table_name, column_name, data_type, is_nullable "
	                    "FROM information_schema.columns "
	                    "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
	                    "ORDER BY table_schema, table_name, ordinal_position";

	auto result = conn.Query(tables_sql);
	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message("Failed to query catalog: " + result->GetError());
		return arrow::Status::OK();
	}

	// Build table info map
	std::unordered_map<string, distributed::TableInfo *> table_map;
	idx_t row_count = 0;

	// Materialize the result to iterate properly
	auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));

	// Iterate through all rows using the Rows() iterator
	for (auto &row : materialized->Collection().Rows()) {
		string schema_name = row.GetValue(0).ToString();
		string table_name = row.GetValue(1).ToString();
		string column_name = row.GetValue(2).ToString();
		string data_type = row.GetValue(3).ToString();
		string is_nullable = row.GetValue(4).ToString();

		string full_table_name = schema_name + "." + table_name;

		// Get or create table info
		if (table_map.find(full_table_name) == table_map.end()) {
			auto *table_info = catalog_resp->add_tables();
			table_info->set_schema_name(schema_name);
			table_info->set_table_name(table_name);
			table_map[full_table_name] = table_info;
		}

		// Add column info
		auto *col_info = table_map[full_table_name]->add_columns();
		col_info->set_name(column_name);
		col_info->set_type(data_type);
		col_info->set_nullable(is_nullable == "YES");
	}

	// Query indexes
	string indexes_sql = "SELECT table_schema, table_name, index_name, sql "
	                     "FROM duckdb_indexes() "
	                     "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";

	auto idx_result = conn.Query(indexes_sql);
	if (!idx_result->HasError()) {
		while (idx_result->Fetch()) {
			auto *index_info = catalog_resp->add_indexes();
			index_info->set_schema_name(idx_result->GetValue(0, 0).ToString());
			index_info->set_table_name(idx_result->GetValue(1, 0).ToString());
			index_info->set_index_name(idx_result->GetValue(2, 0).ToString());
			// Note: We don't parse column_names from SQL for simplicity
			index_info->set_is_unique(false); // Would need to parse from SQL
		}
	}

	resp.set_success(true);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleScanTable(const string &db_path, const distributed::ScanTableRequest &req,
                                                       std::unique_ptr<arrow::flight::FlightDataStream> &stream) {
	string sql =
	    StringUtil::Format("SELECT * FROM %s LIMIT %llu OFFSET %llu", req.table_name(), req.limit(), req.offset());
	auto &conn = GetConnection(db_path);
	auto result = conn.Query(sql);

	if (result->HasError()) {
		return arrow::Status::Invalid("Query error: " + result->GetError());
	}

	std::shared_ptr<arrow::RecordBatchReader> reader;
	ARROW_RETURN_NOT_OK(QueryResultToArrow(*result, reader));

	stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleInsertData(const string &db_path, const std::string &table_name,
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

	auto &conn = GetConnection(db_path);
	auto result = conn.Query(insert_sql);
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
