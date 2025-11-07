#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "server/worker/worker_node.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
namespace duckdb {

WorkerNode::WorkerNode(string worker_id_p, string host_p, int port_p, DuckDB *shared_db)
    : worker_id(std::move(worker_id_p)), host(std::move(host_p)), port(port_p) {
    if (shared_db != nullptr) {
        db = shared_db;
    } else {
		// TODO(hjiang): Pass down db config (i.e., for logging).
        owned_db = make_uniq<DuckDB>(/*path=*/nullptr, /*config=*/nullptr);
        db = owned_db.get();
    }
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
	[[maybe_unused]] auto status = FlightServerBase::Shutdown();
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
	// Ticket contains partition_id for retrieving results.
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(ticket.ticket.data(), ticket.ticket.size())) {
		return arrow::Status::Invalid("Failed to parse ticket");
	}

	if (request.request_case() != distributed::DistributedRequest::kExecutePartition) {
		return arrow::Status::Invalid("DoGet expects ExecutePartition request");
	}

	// Execute the partition and return results.
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
	unique_ptr<QueryResult> result;
	arrow::Status exec_status = arrow::Status::OK();

	

	if (!req.serialized_plan().empty()) {

		std::cerr << "executed distributed query with plan" << std::endl;

		exec_status = ExecuteSerializedPlan(req, result);
	} else {

		std::cerr << "executed distributed query with sql" << std::endl;


		result = conn->Query(req.sql());
	}
	if (!exec_status.ok()) {
		resp.set_success(false);
		resp.set_error_message(exec_status.message());
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Worker %s serialized plan execution failed: %s", worker_id,
		                                     exec_status.ToString()));
		return arrow::Status::OK();
	}
	if (!result) {
		resp.set_success(false);
		resp.set_error_message("Worker produced no query result");
		DUCKDB_LOG_WARN(db_instance, StringUtil::Format("Worker %s produced no result", worker_id));
		return arrow::Status::OK();
	}
	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Worker %s query execution failed: %s", worker_id, result->GetError()));
		return arrow::Status::OK();
	}

	idx_t row_count = 0;
	auto status = QueryResultToArrow(*result, reader, &row_count);
	if (!status.ok()) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Worker %s QueryResultToArrow error: %s", worker_id, status.ToString()));
		return status;
	}

	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_partition();
	exec_resp->set_partition_id(req.partition_id());
	exec_resp->set_row_count(row_count);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s query produced %llu rows", worker_id,
	                                                 static_cast<long long unsigned>(row_count)));
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("Worker %s finished partition %llu", worker_id, req.partition_id()));
	return arrow::Status::OK();
}

arrow::Status WorkerNode::ExecuteSerializedPlan(const distributed::ExecutePartitionRequest &req,
                                               unique_ptr<QueryResult> &result) {
	if (req.column_names_size() != req.column_types_size()) {
		return arrow::Status::Invalid("Mismatched column metadata in ExecutePartitionRequest");
	}

	vector<string> names;
	names.reserve(req.column_names_size());
	for (const auto &name : req.column_names()) {
		names.emplace_back(name);
	}

	vector<LogicalType> types;
	types.reserve(req.column_types_size());
	for (const auto &type_bytes : req.column_types()) {
		MemoryStream type_stream(reinterpret_cast<data_ptr_t>(const_cast<char *>(type_bytes.data())),
		                         type_bytes.size());
		BinaryDeserializer type_deserializer(type_stream);
		type_deserializer.Begin();
		auto type = LogicalType::Deserialize(type_deserializer);
		type_deserializer.End();
		types.emplace_back(std::move(type));
	}

	MemoryStream plan_stream(reinterpret_cast<data_ptr_t>(const_cast<char *>(req.serialized_plan().data())),
	                         req.serialized_plan().size());
	bound_parameter_map_t parameters;
	unique_ptr<LogicalOperator> logical_plan;
	try {
		logical_plan = BinaryDeserializer::Deserialize<LogicalOperator>(plan_stream, *conn->context, parameters);
	} catch (std::exception &ex) {
		return arrow::Status::Invalid(StringUtil::Format("Failed to deserialize plan: %s", ex.what()));
	}
	if (!logical_plan) {
		return arrow::Status::Invalid("Deserialized plan was null");
	}

	PhysicalPlanGenerator plan_gen(*conn->context);
	unique_ptr<PhysicalPlan> physical_plan;
	try {
		physical_plan = plan_gen.Plan(std::move(logical_plan));
	} catch (std::exception &ex) {
		return arrow::Status::Invalid(StringUtil::Format("Failed to generate physical plan: %s", ex.what()));
	}

	auto statement_data = make_uniq<PreparedStatementData>(StatementType::SELECT_STATEMENT);
	statement_data->names = std::move(names);
	statement_data->types = std::move(types);
	statement_data->physical_plan = std::move(physical_plan);

	auto &context = *conn->context;
	auto &collector = PhysicalResultCollector::GetResultCollector(context, *statement_data);
	Executor executor(context);
	executor.Initialize(collector);

	PendingExecutionResult exec_result = PendingExecutionResult::RESULT_NOT_READY;
	while (true) {
		exec_result = executor.ExecuteTask();
		if (exec_result == PendingExecutionResult::RESULT_READY ||
		    exec_result == PendingExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		if (exec_result == PendingExecutionResult::EXECUTION_ERROR) {
			break;
		}
		if (exec_result == PendingExecutionResult::BLOCKED ||
		    exec_result == PendingExecutionResult::NO_TASKS_AVAILABLE) {
			executor.WorkOnTasks();
		}
	}

	if (executor.HasError()) {
		auto error = executor.GetError();
		return arrow::Status::Invalid(error.Message());
	}

	result = executor.GetResult();
	if (!result) {
		return arrow::Status::Invalid("Executor returned no result");
	}

	return arrow::Status::OK();
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

	auto action_result = client->DoAction(action);
	if (!action_result.ok()) {
		return action_result.status();
	}
	auto result_stream = std::move(action_result).ValueOrDie();

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

	// Now get the actual data stream using DoGet with the request as ticket.
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
