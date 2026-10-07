#include "server/driver/client_request_handler.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "query_common.hpp"
#include "server/driver/query_history.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "utils/remote_error.hpp"

#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

ClientRequestHandler::ClientRequestHandler(DatabaseInstance &db_instance_p, QueryHistory &query_history_p)
    : db_instance(db_instance_p), query_history(query_history_p) {
}

arrow::Status ClientRequestHandler::HandleAction(const distributed::DistributedRequest &request,
                                                 ClientRegistration &registration,
                                                 distributed::DistributedResponse &response) {
	auto signature = request.SerializeAsString();
	bool replay = false;
	ARROW_RETURN_NOT_OK(registration.CheckRequestReplay(request, ClientRequestTransport::ACTION, signature, replay));
	if (replay) {
		if (!response.ParseFromString(registration.last_action_response)) {
			return arrow::Status::Invalid("Failed to parse cached operation response");
		}
		return arrow::Status::OK();
	}

	switch (request.request_case()) {
	case distributed::DistributedRequest::kExecuteStatement:
		ARROW_RETURN_NOT_OK(ExecuteStatement(request.execute_statement(), registration, response));
		break;
	case distributed::DistributedRequest::kTableExists:
		ARROW_RETURN_NOT_OK(TableExists(request.table_exists(), registration, response));
		break;
	case distributed::DistributedRequest::kLoadExtension:
		ARROW_RETURN_NOT_OK(LoadExtension(request.load_extension(), registration, response));
		break;
	case distributed::DistributedRequest::kGetQueryExecutionStats:
		query_history.FillStatsResponse(response);
		break;
	default:
		return arrow::Status::Invalid("Unknown request type");
	}
	registration.CacheActionResponse(request, ClientRequestTransport::ACTION, signature, response);
	return arrow::Status::OK();
}

arrow::Status ClientRequestHandler::HandleScan(const distributed::DistributedRequest &request,
                                               ClientRegistration &registration) {
	auto signature = request.SerializeAsString();
	bool replay = false;
	ARROW_RETURN_NOT_OK(registration.CheckRequestReplay(request, ClientRequestTransport::DO_GET, signature, replay));
	if (replay) {
		return arrow::Status::OK();
	}
	// A new request means the previous result can no longer be replayed, so release it before running this scan.
	registration.last_query_schema.reset();
	registration.last_query_batches.clear();
	std::shared_ptr<arrow::Schema> schema;
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	ARROW_RETURN_NOT_OK(ScanTable(request.scan_table(), registration, schema, batches));
	registration.CacheQueryResult(request, signature, std::move(schema), std::move(batches));
	return arrow::Status::OK();
}

arrow::Status ClientRequestHandler::HandleInsert(const distributed::DistributedRequest &request_identity,
                                                 const string &table_name,
                                                 const vector<std::shared_ptr<arrow::RecordBatch>> &batches,
                                                 const string &signature, ClientRegistration &registration,
                                                 distributed::DistributedResponse &response) {
	bool replay = false;
	ARROW_RETURN_NOT_OK(
	    registration.CheckRequestReplay(request_identity, ClientRequestTransport::DO_PUT, signature, replay));
	response.set_success(true);
	if (replay) {
		if (!response.ParseFromString(registration.last_action_response)) {
			return arrow::Status::Invalid("Failed to parse cached insertion response");
		}
		return arrow::Status::OK();
	}
	for (auto &batch : batches) {
		ARROW_RETURN_NOT_OK(InsertData(table_name, batch, registration, response));
		if (!response.success()) {
			break;
		}
	}
	registration.CacheActionResponse(request_identity, ClientRequestTransport::DO_PUT, signature, response);
	return arrow::Status::OK();
}

arrow::Status ClientRequestHandler::ExecuteStatement(const distributed::ExecuteStatementRequest &req,
                                                     ClientRegistration &registration,
                                                     distributed::DistributedResponse &resp) {
	auto sql = StripClientCatalog(req.sql(), req.client_catalog());
	auto result = registration.connection->Query(sql);
	if (result->HasError()) {
		auto &error = result->GetErrorObject();
		resp.set_success(false);
		ToRemoteError(error, *resp.mutable_error());
		return arrow::Status::OK();
	}
	if (!result->client_properties.client_context) {
		result->client_properties.client_context = registration.connection->context.get();
	}

	auto serialization_error = [&](const string &error) {
		resp.set_success(false);
		resp.set_error_message("Remote statement succeeded but its result could not be serialized: " + error);
		return arrow::Status::OK();
	};
	try {
		auto ipc_result = [&]() -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
			std::shared_ptr<arrow::Schema> schema;
			vector<std::shared_ptr<arrow::RecordBatch>> batches;
			ARROW_RETURN_NOT_OK(QueryResultToArrowBatches(*result, schema, batches));

			ARROW_ASSIGN_OR_RAISE(auto output, arrow::io::BufferOutputStream::Create());
			ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(output, schema));
			for (const auto &batch : batches) {
				ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
			}
			ARROW_RETURN_NOT_OK(writer->Close());
			return output->Finish();
		}();
		if (!ipc_result.ok()) {
			return serialization_error(ipc_result.status().ToString());
		}

		resp.set_success(true);
		auto buffer = ipc_result.ValueOrDie();
		resp.mutable_execute_statement()->set_arrow_ipc_result(buffer->data(), buffer->size());
		return arrow::Status::OK();
	} catch (const std::exception &e) {
		return serialization_error(e.what());
	}
}

arrow::Status ClientRequestHandler::LoadExtension(const distributed::LoadExtensionRequest &req,
                                                  ClientRegistration &registration,
                                                  distributed::DistributedResponse &resp) {
	// Execute INSTALL first.
	string sql = "FORCE INSTALL " + req.extension_name();
	if (!req.repository().empty() || !req.version().empty()) {
		if (!req.repository().empty()) {
			sql += " FROM '" + req.repository() + "'";
		}
		if (!req.version().empty()) {
			sql += " VERSION '" + req.version() + "'";
		}
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Install extension with %s", sql));
	auto install_result = registration.connection->Query(sql);
	if (install_result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(
		    StringUtil::Format("Extension %s install failed %s", req.extension_name(), install_result->GetError()));
		return arrow::Status::OK();
	}

	// Then LOAD the extension.
	sql = "LOAD " + req.extension_name();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Load extension with %s", sql));
	auto load_result = registration.connection->Query(sql);
	if (load_result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(
		    StringUtil::Format("Extension %s load failed %s", req.extension_name(), load_result->GetError()));
		return arrow::Status::OK();
	}

	resp.set_success(true);
	resp.mutable_load_extension();
	return arrow::Status::OK();
}

arrow::Status ClientRequestHandler::TableExists(const distributed::TableExistsRequest &req,
                                                ClientRegistration &registration,
                                                distributed::DistributedResponse &resp) {
	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.table_name());

	auto result = registration.connection->Query(sql);

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

arrow::Status ClientRequestHandler::ScanTable(const distributed::ScanTableRequest &req,
                                              ClientRegistration &registration, std::shared_ptr<arrow::Schema> &schema,
                                              vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Handling scan for table: %s", req.table_name()));

	// TODO(hjiang): aggregate pushdown fix:
	// Check if table_name actually contains full SQL (temp hack for testing)
	// In the future, this should come from a dedicated field in the protocol
	string sql;
	string table_identifier = req.table_name();

	// If it looks like SQL (contains SELECT), use it as-is
	// Otherwise, generate SELECT * FROM table
	if (StringUtil::Contains(StringUtil::Upper(table_identifier), "SELECT")) {
		sql = table_identifier;
	} else {
		sql = StringUtil::Format("SELECT * FROM %s", table_identifier);
	}

	if (req.limit() != NO_QUERY_LIMIT) {
		sql += StringUtil::Format(" LIMIT %llu ", req.limit());
	}
	if (req.offset() != NO_QUERY_OFFSET) {
		sql += StringUtil::Format(" OFFSET %llu ", req.offset());
	}

	auto prepared = registration.worker_fragments
	                    ? registration.worker_fragments->PrepareClientQuery(*registration.connection, sql)
	                    : registration.connection->Prepare(sql);
	if (prepared->HasError()) {
		return arrow::Status::Invalid("Query error: " + prepared->GetError());
	}
	// Read-only clients may share a read-write instance with the database's writer.
	if (registration.role != distributed::CLIENT_ROLE_READ_WRITE && !prepared->GetStatementProperties().IsReadOnly()) {
		return arrow::Status::Invalid("Duckherder client is read-only");
	}

	// Start tracking query execution
	QueryExecutionInfo query_info;
	query_info.sql = sql;
	auto query_start = std::chrono::steady_clock::now();                // For duration calculation
	query_info.execution_start_time = std::chrono::system_clock::now(); // Wall-clock timestamp

	// The optimizer routes eligible scan and Join fragments through worker_fragment.
	vector<Value> parameters;
	auto result = prepared->Execute(parameters, /*allow_stream_result=*/false);

	// Calculate total query duration (using steady_clock for accurate elapsed time)
	auto query_end = std::chrono::steady_clock::now();
	query_info.query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start);

	if (registration.worker_fragments != nullptr) {
		for (auto &fragment_info : registration.worker_fragments->TakeExecutions()) {
			query_history.Record(std::move(fragment_info));
		}
	}
	query_history.Record(std::move(query_info));

	if (result->HasError()) {
		return arrow::Status::Invalid("Query error: " + result->GetError());
	}

	if (!result->client_properties.client_context) {
		result->client_properties.client_context = registration.connection->context.get();
	}

	return QueryResultToArrowBatches(*result, schema, batches);
}

arrow::Status ClientRequestHandler::InsertData(const string &table_name,
                                               const std::shared_ptr<arrow::RecordBatch> &batch,
                                               ClientRegistration &registration,
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

	auto result = registration.connection->Query(insert_sql);
	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.set_success(true);
	return arrow::Status::OK();
}

} // namespace duckdb
