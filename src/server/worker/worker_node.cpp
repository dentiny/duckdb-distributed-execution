#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "server/worker/worker_node.hpp"

#include <arrow/array.h>
#include <chrono>
#include <arrow/c/bridge.h>

namespace duckdb {

WorkerNode::WorkerNode(string worker_id_p, string host_p, int port_p, DuckDB *shared_db)
    : worker_id(std::move(worker_id_p)), host(std::move(host_p)), port(port_p) {
	if (shared_db != nullptr) {
		db = shared_db;
	} else {
		owned_db = make_uniq<DuckDB>(/*path=*/nullptr, /*config=*/nullptr);
		db = owned_db.get();
	}
	conn = make_uniq<Connection>(*db);

	// If using shared DB, set the default catalog to "duckling" to match the server.
	if (shared_db != nullptr) {
		auto use_result = conn->Query("USE duckling;");
		if (use_result->HasError()) {
			throw InternalException(
			    StringUtil::Format("Worker %s failed to USE duckling: %s", worker_id, use_result->GetError()));
		}
	}
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
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s received partition request", worker_id));
	ARROW_RETURN_NOT_OK(HandleExecutePartition(request.execute_partition(), response, reader));
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Worker %s returning partition stream", worker_id));

	if (!reader) {
		return arrow::Status::Invalid("Failed to create RecordBatchReader: execution produced no reader");
	}

	*stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);

	return arrow::Status::OK();
}

// STEP 3: Execute a pipeline task with state tracking and metrics
arrow::Status WorkerNode::ExecutePipelineTask(const distributed::ExecutePartitionRequest &req,
                                               TaskExecutionState &task_state,
                                               unique_ptr<QueryResult> &result) {
	auto &db_instance = *db->instance.get();
	auto start_time = std::chrono::high_resolution_clock::now();
	
	// Initialize task state
	task_state.task_id = req.partition_id();
	task_state.total_tasks = req.total_partitions();
	task_state.task_sql = req.sql();
	task_state.completed = false;
	task_state.rows_processed = 0;
	
	std::cerr << "\n[STEP 3] Worker " << worker_id << ": Executing pipeline task " 
	          << task_state.task_id << "/" << task_state.total_tasks << std::endl;
	
	arrow::Status exec_status = arrow::Status::OK();
	
	// STEP 3 NOTE: Plan-based execution temporarily disabled
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
	
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("🔧 [STEP3] Worker %s: Task %llu - Using SQL-BASED execution (plan-based execution disabled until physical plan support)",
	                                  worker_id, task_state.task_id));
	
	// Execute task using SQL-based execution
	if (!result && !req.sql().empty()) {
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("🔄 [STEP3] Worker %s: Task %llu - SQL-BASED execution", 
		                                  worker_id, task_state.task_id));
		std::cerr << "  Execution Mode: SQL-BASED" << std::endl;
		std::cerr << "  SQL: " << req.sql() << std::endl;
		
		result = conn->Query(req.sql());
		
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("✅ [STEP3] Worker %s: Task %llu - SQL-BASED execution completed", 
		                                  worker_id, task_state.task_id));
	}
	
	// Record execution time
	auto end_time = std::chrono::high_resolution_clock::now();
	task_state.execution_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
		end_time - start_time).count();
	
	// Validate result
	if (!result) {
		task_state.completed = false;
		task_state.error_message = "No query result produced";
		std::cerr << "  Status: FAILED (no result)" << std::endl;
		return arrow::Status::Invalid("Worker produced no query result for task");
	}
	
	if (result->HasError()) {
		task_state.completed = false;
		task_state.error_message = result->GetError();
		std::cerr << "  Status: FAILED (" << result->GetError() << ")" << std::endl;
		return arrow::Status::Invalid(StringUtil::Format("Task execution failed: %s", result->GetError()));
	}
	
	// Task completed successfully
	task_state.completed = true;
	
	// Record row count (will be updated after Arrow conversion)
	auto materialized = dynamic_cast<MaterializedQueryResult *>(result.get());
	if (materialized) {
		task_state.rows_processed = materialized->RowCount();
		
		// DEBUGGING: Print actual results from this worker
		std::cerr << "  Status: SUCCESS" << std::endl;
		std::cerr << "  Rows Processed: " << task_state.rows_processed << std::endl;
		std::cerr << "  Execution Time: " << task_state.execution_time_ms << "ms" << std::endl;
		
		// Print sample of results for debugging
		if (task_state.rows_processed > 0) {
			std::cerr << "  [DEBUG] First few rows from worker:" << std::endl;
			auto &collection = materialized->Collection();
			ColumnDataScanState scan_state;
			collection.InitializeScan(scan_state);
			DataChunk debug_chunk;
			debug_chunk.Initialize(Allocator::DefaultAllocator(), materialized->types);
			
			if (collection.Scan(scan_state, debug_chunk) && debug_chunk.size() > 0) {
				idx_t rows_to_show = std::min(static_cast<idx_t>(3), debug_chunk.size());
				for (idx_t row = 0; row < rows_to_show; row++) {
					std::cerr << "    Row " << row << ": ";
					for (idx_t col = 0; col < debug_chunk.ColumnCount(); col++) {
						if (col > 0) std::cerr << ", ";
						std::cerr << debug_chunk.GetValue(col, row).ToString();
					}
					std::cerr << std::endl;
				}
			}
		}
	} else {
		std::cerr << "  Status: SUCCESS" << std::endl;
		std::cerr << "  Rows Processed: " << task_state.rows_processed << std::endl;
		std::cerr << "  Execution Time: " << task_state.execution_time_ms << "ms" << std::endl;
	}
	
	DUCKDB_LOG_DEBUG(db_instance,
	                StringUtil::Format("✅ [STEP3] Worker %s: Task %llu completed - %llu rows in %llu ms",
	                                  worker_id, task_state.task_id,
	                                  static_cast<long long unsigned>(task_state.rows_processed),
	                                  static_cast<long long unsigned>(task_state.execution_time_ms)));
	
	return arrow::Status::OK();
}

arrow::Status WorkerNode::HandleExecutePartition(const distributed::ExecutePartitionRequest &req,
                                                 distributed::DistributedResponse &resp,
                                                 std::shared_ptr<arrow::RecordBatchReader> &reader) {
	auto &db_instance = *db->instance.get();
	
	// STEP 3: Create task execution state
	TaskExecutionState task_state;
	task_state.task_id = req.partition_id();
	task_state.total_tasks = req.total_partitions();
	current_task = make_uniq<TaskExecutionState>(task_state);
	
	// Log the task assignment
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("🔨 [STEP3-START] Worker %s: Received task %llu/%llu", 
	                                  worker_id, req.partition_id(), req.total_partitions()));
	
	std::cerr << "\n[WORKER " << worker_id << "] Received partition " << req.partition_id() << "/" << req.total_partitions() << std::endl;
	
	if (!req.sql().empty()) {
		std::cerr << "[WORKER " << worker_id << "] SQL: " << req.sql() << std::endl;
	}
	
	// STEP 3: Execute the pipeline task with state tracking
	unique_ptr<QueryResult> result;
	auto exec_status = ExecutePipelineTask(req, task_state, result);
	
	if (!exec_status.ok()) {
		resp.set_success(false);
		resp.set_error_message(task_state.error_message);
		current_task.reset();
		return exec_status;
	}

	// Convert result to Arrow format
	// This represents the LocalState output from this worker node
	idx_t row_count = 0;
	auto status = QueryResultToArrow(*result, reader, &row_count);
	if (!status.ok()) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Worker %s QueryResultToArrow error: %s", worker_id, status.ToString()));
		return status;
	}

	// Update task state with final row count
	task_state.rows_processed = row_count;
	
	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_partition();
	exec_resp->set_partition_id(req.partition_id());
	exec_resp->set_row_count(row_count);

	// STEP 3: Log task completion with metrics
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("✅ [STEP3-DONE] Worker %s: Task %llu/%llu COMPLETE - %llu rows in %llu ms", 
	                                  worker_id, req.partition_id(), req.total_partitions(),
	                                  static_cast<long long unsigned>(row_count),
	                                  static_cast<long long unsigned>(task_state.execution_time_ms)));
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("📤 [WORKER-DONE] Worker %s: Sending %llu rows back to coordinator for merge", 
	                                  worker_id, static_cast<long long unsigned>(row_count)));
	
	std::cerr << "[WORKER " << worker_id << "] COMPLETE: Returning " << row_count << " rows" << std::endl;

	// STEP 3: Clear current task state
	current_task.reset();
	
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

	auto &db_instance = *db->instance.get();
	
	// Begin a transaction before deserializing the plan
	// Note: Deserialization requires an active transaction to resolve table bindings
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("📦 [DESERIALIZE] Worker %s: Beginning transaction for plan deserialization", worker_id));
	conn->BeginTransaction();

	// Deserialize the logical plan
	// This plan contains the partition predicate embedded in it by the coordinator
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("📦 [DESERIALIZE] Worker %s: Deserializing logical plan (%llu bytes)", 
	                                  worker_id, static_cast<long long unsigned>(req.serialized_plan().size())));
	MemoryStream plan_stream(reinterpret_cast<data_ptr_t>(const_cast<char *>(req.serialized_plan().data())),
	                         req.serialized_plan().size());
	bound_parameter_map_t parameters;
	unique_ptr<LogicalOperator> logical_plan;
	try {
		logical_plan = BinaryDeserializer::Deserialize<LogicalOperator>(plan_stream, *conn->context, parameters);
		DUCKDB_LOG_DEBUG(db_instance, 
		                StringUtil::Format("✅ [DESERIALIZE] Worker %s: Logical plan deserialized successfully", worker_id));
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance, 
		                StringUtil::Format("❌ [DESERIALIZE] Worker %s: Deserialization failed: %s", worker_id, ex.what()));
		conn->Rollback();
		return arrow::Status::Invalid(StringUtil::Format("Failed to deserialize plan: %s", ex.what()));
	}
	if (!logical_plan) {
		DUCKDB_LOG_WARN(db_instance, 
		                StringUtil::Format("❌ [DESERIALIZE] Worker %s: Deserialized plan is null", worker_id));
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
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("⚙️  [EXECUTE] Worker %s: Executing logical plan (LocalState execution)", worker_id));
	auto statement = make_uniq<LogicalPlanStatement>(std::move(logical_plan));
	auto materialized = conn->Query(std::move(statement));

	// Commit the transaction
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("💾 [EXECUTE] Worker %s: Committing transaction", worker_id));
	conn->Commit();

	if (materialized->HasError()) {
		DUCKDB_LOG_WARN(db_instance, 
		                StringUtil::Format("❌ [EXECUTE] Worker %s: Query execution error: %s", worker_id, materialized->GetError()));
		return arrow::Status::Invalid(materialized->GetError());
	}

	// Validate and fix column metadata
	if (materialized->types.size() != types.size()) {
		DUCKDB_LOG_WARN(db_instance, 
		                StringUtil::Format("❌ [EXECUTE] Worker %s: Column count mismatch (expected: %llu, got: %llu)", 
		                                  worker_id, static_cast<long long unsigned>(types.size()), 
		                                  static_cast<long long unsigned>(materialized->types.size())));
		return arrow::Status::Invalid("Worker result column count mismatch with expected types");
	}
	
	DUCKDB_LOG_DEBUG(db_instance, 
	                StringUtil::Format("✅ [EXECUTE] Worker %s: Plan execution completed, result ready for transmission", worker_id));
	materialized->types = std::move(types);
	if (materialized->names.size() == names.size()) {
		materialized->names = std::move(names);
	}
	result = std::move(materialized);
	return arrow::Status::OK();
}

arrow::Status WorkerNode::QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader,
                                             idx_t *row_count) {
	// Convert DuckDB QueryResult to Arrow RecordBatchReader
	// 
	// This method serializes the LocalState output from this worker node
	// to Arrow format for transmission back to the coordinator.
	//
	// In DuckDB's parallel execution model:
	// - Each thread produces results in its LocalSinkState
	// - These results are typically DataChunks or ColumnDataCollections
	// - The Combine() method would merge these into GlobalSinkState
	//
	// In distributed execution:
	// - This worker's QueryResult represents LocalState output
	// - We serialize it to Arrow for network transmission
	// - Coordinator receives and combines all worker outputs (GlobalState semantics)
	// - This maintains the same aggregation pattern as thread-level parallelism
	
	auto &db_instance = *db->instance.get();
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, result.client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));

	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	idx_t count = 0;

	// Fetch and convert each chunk from the query result
	// Each chunk represents a batch of rows processed by this worker node
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
