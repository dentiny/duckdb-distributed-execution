#include "arrow_utils.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "server/object_storage_database.hpp"
#include "server/startup_sql.hpp"
#include "server/validation.hpp"
#include "server/worker/row_group_range_scan.hpp"
#include "server/worker/worker_node.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <chrono>

namespace duckdb {

WorkerNode::WorkerNode(string worker_id_p, string host_p, int port_p)
    : worker_id(std::move(worker_id_p)), host(std::move(host_p)), port(port_p) {
	db = make_uniq<DuckDB>(/*path=*/nullptr, /*config=*/nullptr);
	// Workers need core functions when created from a loadable extension.
	db->LoadStaticExtension<CoreFunctionsExtension>();
	auto startup_status = RunStartupSQL(*db);
	if (!startup_status.ok()) {
		throw IOException(startup_status.ToString());
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
		return arrow::Status::Invalid(
		    StringUtil::Format("Unknown request type for worker: %d", static_cast<int>(request.request_case())));
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
	ARROW_RETURN_NOT_OK(HandleExecutePartition(request.execute_partition(), response, reader));

	if (!reader) {
		return arrow::Status::Invalid("Failed to create RecordBatchReader: execution produced no reader");
	}

	*stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);

	return arrow::Status::OK();
}

// Execute a pipeline task.
arrow::Status WorkerNode::ExecutePipelineTask(const distributed::ExecutePartitionRequest &req, Connection &task_conn,
                                              unique_ptr<QueryResult> &result) {
	arrow::Status exec_status = arrow::Status::OK();

	// TODO(hjiang): Plan-based execution temporarily disabled
	//
	// Issue: We're currently serializing LOGICAL plans (which have already been optimized
	// on the coordinator). When workers deserialize and try to execute them, DuckDB
	// runs the optimizer again, which fails on already-bound expressions.
	//
	// Solution (for future steps): Serialize and deserialize PHYSICAL plans instead,
	// which can be executed directly without re-optimization.
	//
	// For now, we rely on SQL-based execution which works perfectly.

	// Disabled plan-based execution:
	// if (!req.serialized_plan().empty()) {
	//     exec_status = ExecuteSerializedPlan(req, result);
	//     ...
	// }

	// Execute task using SQL-based execution.
	if (!result && !req.sql().empty()) {
		// Stream the result so it is converted to Arrow as it is produced instead of being materialized first.
		result = task_conn.SendQuery(req.sql());
	}

	// Validate result.
	if (!result) {
		return arrow::Status::Invalid("Worker produced no query result for task");
	}
	if (result->HasError()) {
		return arrow::Status::Invalid(StringUtil::Format("Task execution failed: %s", result->GetError()));
	}

	return arrow::Status::OK();
}

arrow::Status WorkerNode::HandleExecutePartition(const distributed::ExecutePartitionRequest &req,
                                                 distributed::DistributedResponse &resp,
                                                 std::shared_ptr<arrow::RecordBatchReader> &reader) {
	// Object storage tasks run on their own session of this worker's instance for that database.
	ARROW_RETURN_NOT_OK(ValidateRequest(req.storage_config()));
	if (req.storage_config().storage_case() == distributed::StorageConfig::kInMemory) {
		return arrow::Status::Invalid("Workers cannot execute on control-node in-memory object storage");
	}
	ARROW_ASSIGN_OR_RAISE(auto object_storage_database, GetOrOpenObjectStorageDatabase(req.storage_config()));
	ARROW_ASSIGN_OR_RAISE(auto task_conn, object_storage_database->Connect());

	// Execute the pipeline task with state tracking
	unique_ptr<QueryResult> result;
	auto exec_status = ExecutePipelineTask(req, *task_conn, result);

	if (!exec_status.ok()) {
		resp.set_success(false);
		resp.set_error_message(exec_status.message());
		return exec_status;
	}

	// Convert result to Arrow format.
	// This represents the LocalState output from this worker node; the coordinator combines the outputs of all
	// workers, mirroring how thread-local sink states are combined into a global state.
	idx_t row_count = 0;
	auto status = QueryResultToArrowReader(*result, *task_conn->context, reader, &row_count);
	if (!status.ok()) {
		return status;
	}

	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_partition();
	exec_resp->set_partition_id(req.partition_id());
	exec_resp->set_row_count(row_count);

	return arrow::Status::OK();
}

arrow::Status WorkerNode::ExecuteSerializedPlan(const distributed::ExecutePartitionRequest &req,
                                                unique_ptr<QueryResult> &result) {
	if (req.column_names_size() != req.column_types_size()) {
		return arrow::Status::Invalid("Mismatched column metadata in ExecutePartitionRequest");
	}

	// Extract column metadata
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

	// Begin a transaction before deserializing the plan
	// Note: Deserialization requires an active transaction to resolve table bindings
	conn->BeginTransaction();

	// Deserialize the logical plan
	// This plan contains the partition predicate embedded in it by the coordinator
	MemoryStream plan_stream(reinterpret_cast<data_ptr_t>(const_cast<char *>(req.serialized_plan().data())),
	                         req.serialized_plan().size());
	bound_parameter_map_t parameters;
	unique_ptr<LogicalOperator> logical_plan =
	    BinaryDeserializer::Deserialize<LogicalOperator>(plan_stream, *conn->context, parameters);
	if (!logical_plan) {
		conn->Rollback();
		return arrow::Status::Invalid("Deserialized plan was null");
	}

	// Execute the plan using DuckDB's query execution infrastructure
	//
	// Execution flow (mapping to parallel execution model):
	// 1. LogicalPlanStatement is converted to a physical plan
	// 2. Physical plan is executed via Executor/Pipeline infrastructure
	// 3. Each physical operator has Source/Sink semantics:
	//    - Source operators: Produce data chunks (e.g., TableScan with partition filter)
	//    - Sink operators: Consume data chunks (e.g., ResultCollector)
	// 4. For parallel operators:
	//    - GetLocalSinkState() creates per-thread (now per-worker) state
	//    - Sink() processes data chunks into LocalSinkState
	//    - Combine() would merge LocalSinkState into GlobalSinkState (on coordinator)
	//    - Finalize() produces final result (on coordinator)
	//
	// In distributed mode:
	// - This worker node acts as ONE thread/execution unit
	// - We execute our partition and return LocalState output
	// - Coordinator acts as the GlobalState aggregator
	auto statement = make_uniq<LogicalPlanStatement>(std::move(logical_plan));
	auto materialized = conn->Query(std::move(statement));

	// Commit the transaction
	conn->Commit();

	if (materialized->HasError()) {
		return arrow::Status::Invalid(materialized->GetError());
	}

	// Validate and fix column metadata
	if (materialized->types.size() != types.size()) {
		return arrow::Status::Invalid("Worker result column count mismatch with expected types");
	}

	materialized->types = std::move(types);
	if (materialized->names.size() == names.size()) {
		materialized->names = std::move(names);
	}
	result = std::move(materialized);
	return arrow::Status::OK();
}

arrow::Result<ObjectStorageDatabase *>
WorkerNode::GetOrOpenObjectStorageDatabase(const distributed::StorageConfig &config) {
	const concurrency::lock_guard<concurrency::mutex> lock(object_storage_mutex);
	auto &instance = object_storage_databases[ObjectStorageDatabase::GetKey(config)];
	if (!instance) {
		auto database_result = ObjectStorageDatabase::Create(config, AccessMode::READ_ONLY);
		if (!database_result.ok()) {
			return arrow::Status::IOError("Worker ", worker_id, " failed to attach ", config.database_uri(), ": ",
			                              database_result.status().message());
		}
		instance = std::move(database_result).ValueOrDie();
		OptimizerExtension::Register(DBConfig::GetConfig(*instance->GetInstance().instance),
		                             GetRowGroupRangeScanExtension());
		DUCKDB_LOG_DEBUG(*instance->GetInstance().instance,
		                 StringUtil::Format("Worker %s attached %s", worker_id, config.database_uri()));
	}
	return instance.get();
}

} // namespace duckdb
