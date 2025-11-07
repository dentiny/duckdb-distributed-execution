#include "worker_node.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
namespace duckdb {

// ============================================================================
// WorkerNode Implementation
// ============================================================================

WorkerNode::WorkerNode(string worker_id_p, string host_p, int port_p)
    : worker_id(std::move(worker_id_p)), host(std::move(host_p)), port(port_p) {
	db = make_uniq<DuckDB>(nullptr, nullptr);
	conn = make_uniq<Connection>(*db);
}

arrow::Status WorkerNode::Start() {
	arrow::flight::Location location;
	ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp(host, port));

	arrow::flight::FlightServerOptions options(location);
	ARROW_RETURN_NOT_OK(Init(options));

	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s started on %s:%d", worker_id, host, port));

	return arrow::Status::OK();
}

void WorkerNode::Shutdown() {
	auto status = FlightServerBase::Shutdown();
	(void)status; // Ignore shutdown errors
}

string WorkerNode::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host, port);
}

arrow::Status WorkerNode::DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
                                   std::unique_ptr<arrow::flight::ResultStream> *result) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(action.body->data(), action.body->size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	distributed::DistributedResponse response;
	response.set_success(true);

	switch (request.request_case()) {
	case distributed::DistributedRequest::kExecutePartition:
		response.mutable_execute_partition();
		break;
	case distributed::DistributedRequest::kWorkerHeartbeat: {
		auto *hb_resp = response.mutable_worker_heartbeat();
		hb_resp->set_healthy(true);
		break;
	}
	default:
		return arrow::Status::Invalid("Unknown request type for worker");
	}

	std::string response_data = response.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(response_data);

	std::vector<arrow::flight::Result> results;
	results.emplace_back(arrow::flight::Result {buffer});
	*result = std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));

	return arrow::Status::OK();
}

arrow::Status WorkerNode::DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
                                std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
	// Ticket contains partition_id for retrieving results
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(ticket.ticket.data(), ticket.ticket.size())) {
		return arrow::Status::Invalid("Failed to parse ticket");
	}

	if (request.request_case() != distributed::DistributedRequest::kExecutePartition) {
		return arrow::Status::Invalid("DoGet expects ExecutePartition request");
	}

	// Execute the partition and return results
	distributed::DistributedResponse response;
	std::shared_ptr<arrow::RecordBatchReader> reader;
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s received partition request", worker_id));
	ARROW_RETURN_NOT_OK(HandleExecutePartition(request.execute_partition(), response, reader));
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s returning partition stream", worker_id));

	*stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);

	return arrow::Status::OK();
}

arrow::Status WorkerNode::HandleExecutePartition(const distributed::ExecutePartitionRequest &req,
                                                 distributed::DistributedResponse &resp,
                                                 std::shared_ptr<arrow::RecordBatchReader> &reader) {
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s executing partition %llu/%llu (sql: %s)", worker_id,
	                                                 req.partition_id(), req.total_partitions(), req.sql()));

	string temp_table = StringUtil::Format("temp_partition_%llu", req.partition_id());

	// Step 1: Create a temporary table from the partition data
	if (!req.partition_data().empty()) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s partition payload size %llu bytes", worker_id,
		                                                 static_cast<long long unsigned>(req.partition_data().size())));
		// Deserialize Arrow IPC data
		auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(req.partition_data().data()),
		                                              req.partition_data().size());
		auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

		ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));

		// Get schema from first batch
		ARROW_ASSIGN_OR_RAISE(auto first_batch, ipc_reader->Next());
		if (!first_batch) {
			// Empty partition - create empty table and return empty result
			resp.set_success(true);
			auto *exec_resp = resp.mutable_execute_partition();
			exec_resp->set_partition_id(req.partition_id());
			exec_resp->set_row_count(0);
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s received empty partition", worker_id));
			return arrow::Status::OK();
		}

		// Build CREATE TABLE statement from schema
		auto schema = first_batch->schema();
		std::stringstream create_sql;
		create_sql << "CREATE TEMP TABLE " << temp_table << " (";
		for (int i = 0; i < schema->num_fields(); i++) {
			if (i > 0)
				create_sql << ", ";
			auto field = schema->field(i);
			create_sql << field->name() << " ";

			// Map Arrow types to DuckDB types (simplified)
			auto arrow_type = field->type();
			if (arrow_type->id() == arrow::Type::INT32) {
				create_sql << "INTEGER";
			} else if (arrow_type->id() == arrow::Type::INT64) {
				create_sql << "BIGINT";
			} else if (arrow_type->id() == arrow::Type::DOUBLE) {
				create_sql << "DOUBLE";
			} else if (arrow_type->id() == arrow::Type::STRING) {
				create_sql << "VARCHAR";
			} else {
				create_sql << "VARCHAR"; // Default fallback
			}
		}
		create_sql << ")";

		auto create_result = conn->Query(create_sql.str());
		if (create_result->HasError()) {
			resp.set_success(false);
			resp.set_error_message("Failed to create temp table: " + create_result->GetError());
			DUCKDB_LOG_WARN(db_instance, StringUtil::Format("Worker %s failed to create temp table: %s", worker_id,
			                                                create_result->GetError()));
			return arrow::Status::OK();
		}

		// Insert data from all batches
		// Process first batch and subsequent batches
		vector<std::shared_ptr<arrow::RecordBatch>> batches;
		batches.push_back(first_batch);

		while (true) {
			ARROW_ASSIGN_OR_RAISE(auto batch, ipc_reader->Next());
			if (!batch) {
				break;
			}
			batches.push_back(batch);
		}

		// Insert batches into temp table
		for (const auto &batch : batches) {
			std::stringstream insert_sql;
			insert_sql << "INSERT INTO " << temp_table << " VALUES ";

			for (int64_t row = 0; row < batch->num_rows(); row++) {
				if (row > 0)
					insert_sql << ", ";
				insert_sql << "(";

				for (int col = 0; col < batch->num_columns(); col++) {
					if (col > 0)
						insert_sql << ", ";

					auto array = batch->column(col);
					if (array->IsNull(row)) {
						insert_sql << "NULL";
					} else {
						// Simple value extraction
						auto type_id = array->type_id();
						if (type_id == arrow::Type::INT32) {
							auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
							insert_sql << int_array->Value(row);
						} else if (type_id == arrow::Type::INT64) {
							auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
							insert_sql << int_array->Value(row);
						} else if (type_id == arrow::Type::DOUBLE) {
							auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
							insert_sql << double_array->Value(row);
						} else if (type_id == arrow::Type::STRING) {
							auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
							insert_sql << "'" << string_array->GetString(row) << "'";
						} else {
							insert_sql << "NULL";
						}
					}
				}
				insert_sql << ")";
			}

			auto insert_result = conn->Query(insert_sql.str());
			if (insert_result->HasError()) {
				DUCKDB_LOG_WARN(db_instance, StringUtil::Format("Worker %s insert failed: %s", worker_id,
				                                                insert_result->GetError()));
			}
		}
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Worker %s loaded partition into temp table %s", worker_id, temp_table));
	}

	// Step 2: Rewrite SQL to query the temp table instead
	// For simple queries like "SELECT * FROM table", replace table name with temp_table
	string modified_sql = req.sql();
	// TODO: More sophisticated SQL rewriting - for now assume simple SELECT
	// This is a hack - in production we'd parse the SQL properly
	auto from_pos = modified_sql.find("FROM");
	if (from_pos != string::npos) {
		size_t table_start = from_pos + 5; // Skip "FROM "
		while (table_start < modified_sql.length() && std::isspace(modified_sql[table_start])) {
			table_start++;
		}
		size_t table_end = table_start;
		while (table_end < modified_sql.length() && !std::isspace(modified_sql[table_end]) &&
		       modified_sql[table_end] != ';') {
			table_end++;
		}

		if (table_end > table_start) {
			modified_sql.replace(table_start, table_end - table_start, temp_table);
		}
	}

	// Execute the modified SQL query
	auto result = conn->Query(modified_sql);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Worker %s query execution failed: %s", worker_id, result->GetError()));
		return arrow::Status::OK();
	}

	// Clean up temp table
	idx_t row_count = 0;
	auto status = QueryResultToArrow(*result, reader, &row_count);
	if (!status.ok()) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Worker %s QueryResultToArrow error: %s", worker_id, status.ToString()));
		return status;
	}
	conn->Query("DROP TABLE IF EXISTS " + temp_table);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s query produced %llu rows", worker_id,
	                                                 static_cast<long long unsigned>(row_count)));
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("Worker %s finished partition %llu", worker_id, req.partition_id()));

	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_partition();
	exec_resp->set_partition_id(req.partition_id());
	exec_resp->set_row_count(row_count);

	return status;
}

arrow::Status WorkerNode::QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader,
                                             idx_t *row_count) {
	auto &db_instance = *db->instance.get();
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, result.client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));

	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	idx_t count = 0;

	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s converting chunk size %llu", worker_id,
		                                                 static_cast<long long unsigned>(chunk->size())));
		ArrowArray arrow_array;
		auto extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(*result.client_properties.client_context, result.types);
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, result.client_properties, extension_types);

		auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
		if (!batch_result.ok()) {
			return arrow::Status::Invalid("Failed to import Arrow batch");
		}

		auto batch = batch_result.ValueOrDie();
		count += batch->num_rows();
		batches.emplace_back(batch);
	}

	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches, schema));
	if (row_count) {
		*row_count = count;
	}
	return arrow::Status::OK();
}

// ============================================================================
// WorkerNodeClient Implementation
// ============================================================================

WorkerNodeClient::WorkerNodeClient(const string &location) : location(location) {
}

arrow::Status WorkerNodeClient::Connect() {
	arrow::flight::Location flight_location;
	ARROW_ASSIGN_OR_RAISE(flight_location, arrow::flight::Location::Parse(location));
	ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(flight_location));
	return arrow::Status::OK();
}

arrow::Status WorkerNodeClient::ExecutePartition(const distributed::ExecutePartitionRequest &request,
                                                 std::unique_ptr<arrow::flight::FlightStreamReader> &stream) {
	distributed::DistributedRequest req;
	*req.mutable_execute_partition() = request;

	std::string req_data = req.SerializeAsString();
	arrow::flight::Action action {"execute_partition", arrow::Buffer::FromString(req_data)};

	// DoAction now returns Result<unique_ptr<ResultStream>>
	auto action_result = client->DoAction(action);
	if (!action_result.ok()) {
		return action_result.status();
	}
	auto result_stream = std::move(action_result).ValueOrDie();

	// Get the response
	auto next_result = result_stream->Next();
	if (!next_result.ok()) {
		return next_result.status();
	}
	auto result = std::move(next_result).ValueOrDie();
	if (!result) {
		return arrow::Status::Invalid("No response from worker");
	}

	distributed::DistributedResponse response;
	if (!response.ParseFromArray(result->body->data(), result->body->size())) {
		return arrow::Status::Invalid("Failed to parse response");
	}

	if (!response.success()) {
		return arrow::Status::Invalid("Worker execution failed: " + response.error_message());
	}

	// Now get the actual data stream using DoGet with the request as ticket
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;
	auto doget_result = client->DoGet(ticket);
	if (!doget_result.ok()) {
		return doget_result.status();
	}
	stream = std::move(doget_result).ValueOrDie();

	return arrow::Status::OK();
}

} // namespace duckdb
