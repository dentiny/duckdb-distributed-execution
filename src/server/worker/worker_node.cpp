#include "arrow_utils.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/object_storage_database.hpp"
#include "server/startup_sql.hpp"
#include "server/validation.hpp"
#include "server/worker/row_group_range_scan.hpp"
#include "server/worker/worker_node.hpp"

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
	// The ticket carries the complete ExecutePartition request.
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

arrow::Status WorkerNode::ExecutePlanFragment(const distributed::ExecutePartitionRequest &req, Connection &task_conn,
                                              unique_ptr<QueryResult> &result) {
	const auto build_id = PlanSerializer::BuildId();
	if (req.build_id() != build_id) {
		return arrow::Status::Invalid("Plan fragment from build ", req.build_id(), " cannot run on worker ", worker_id,
		                              " of build ", build_id);
	}
	unique_ptr<LogicalOperator> plan;
	try {
		plan = PlanSerializer::DeserializeLogicalPlan(*task_conn.context, req.serialized_plan());
	} catch (const std::exception &ex) {
		return arrow::Status::Invalid("Plan fragment deserialization failed: ", ErrorData(ex).Message());
	}
	result = task_conn.SendQuery(make_uniq<LogicalPlanStatement>(std::move(plan)));
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

	unique_ptr<QueryResult> result;
	auto exec_status = ExecutePlanFragment(req, *task_conn, result);

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
