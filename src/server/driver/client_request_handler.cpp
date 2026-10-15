#include "server/driver/client_request_handler.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "server/driver/query_history.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "utils/remote_error.hpp"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

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

arrow::Status ClientRequestHandler::HandleQuery(const distributed::DistributedRequest &request,
                                                ClientRegistration &registration) {
	auto signature = request.SerializeAsString();
	bool replay = false;
	ARROW_RETURN_NOT_OK(registration.CheckRequestReplay(request, ClientRequestTransport::DO_GET, signature, replay));
	if (replay) {
		return arrow::Status::OK();
	}
	// A new request means the previous result can no longer be replayed, so release it before running this query.
	registration.last_query_schema.reset();
	registration.last_query_batches.clear();
	std::shared_ptr<arrow::Schema> schema;
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	ARROW_RETURN_NOT_OK(RunQuery(request.query(), registration, schema, batches));
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
	if (!batches.empty()) {
		InsertData(table_name, batches, registration, response);
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
	const auto extension_name = KeywordHelper::WriteQuoted(req.extension_name());
	// Execute INSTALL first.
	auto sql = StringUtil::Format("FORCE INSTALL %s", extension_name);
	if (!req.repository().empty()) {
		sql += StringUtil::Format(" FROM %s", KeywordHelper::WriteQuoted(req.repository()));
	}
	if (!req.version().empty()) {
		sql += StringUtil::Format(" VERSION %s", KeywordHelper::WriteQuoted(req.version()));
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
	sql = StringUtil::Format("LOAD %s", extension_name);
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
	auto sql = StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
	                              KeywordHelper::WriteQuoted(req.table_name()));

	auto result = registration.connection->Query(sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	resp.mutable_table_exists()->set_exists(result->GetValue(0, 0).GetValue<int64_t>() > 0);
	resp.set_success(true);
	return arrow::Status::OK();
}

arrow::Status ClientRequestHandler::RunQuery(const distributed::QueryRequest &req, ClientRegistration &registration,
                                             std::shared_ptr<arrow::Schema> &schema,
                                             vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	const auto &sql = req.sql();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Handling query: %s", sql));

	auto prepared = registration.connection->Prepare(sql);
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

void ClientRequestHandler::InsertData(const string &table_name,
                                      const vector<std::shared_ptr<arrow::RecordBatch>> &batches,
                                      ClientRegistration &registration, distributed::DistributedResponse &resp) {
	try {
		// Parse only the target name; batch values stay typed throughout ingestion.
		Parser parser;
		parser.ParseQuery(StringUtil::Format("INSERT INTO %s DEFAULT VALUES", table_name));
		if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::INSERT_STATEMENT) {
			throw InvalidInputException("DoPut requires a single table name");
		}
		auto &insert = parser.statements[0]->Cast<InsertStatement>();
		// Like SQL binding, a two-part name can refer to catalog.table instead of schema.table.
		if (insert.catalog.empty() && !insert.schema.empty() &&
		    !registration.connection->TableInfo(insert.catalog, insert.schema, insert.table)) {
			insert.catalog = std::move(insert.schema);
			insert.schema = DEFAULT_SCHEMA;
		}
		Appender appender(*registration.connection, insert.catalog, insert.schema, insert.table);
		for (auto &batch : batches) {
			DataChunk chunk;
			ArrowRecordBatchToDataChunk(*registration.connection->context, *batch, chunk, &appender.GetActiveTypes());
			appender.AppendDataChunk(chunk);
		}
		appender.Close();
	} catch (const std::exception &ex) {
		resp.set_success(false);
		auto error = ErrorData(ex);
		resp.set_error_message(error.Message());
		ToRemoteError(error, *resp.mutable_error());
		return;
	}
	resp.set_success(true);
}

} // namespace duckdb
