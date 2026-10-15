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

	std::shared_ptr<arrow::RecordBatchReader> reader;
	ARROW_RETURN_NOT_OK(HandleExecutePartition(request.execute_partition(), reader));

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
                                                 std::shared_ptr<arrow::RecordBatchReader> &reader) {
	// Object storage tasks run on their own session of this worker's instance for that database.
	ARROW_RETURN_NOT_OK(ValidateRequest(req.storage_config()));
	if (req.storage_config().storage_case() == distributed::StorageConfig::kInMemory) {
		return arrow::Status::Invalid("Workers cannot execute on control-node in-memory object storage");
	}
	ARROW_ASSIGN_OR_RAISE(auto object_storage_database, GetOrOpenObjectStorageDatabase(req.storage_config()));
	ARROW_ASSIGN_OR_RAISE(auto task_conn, object_storage_database->Connect());

	unique_ptr<QueryResult> result;
	ARROW_RETURN_NOT_OK(ExecutePlanFragment(req, *task_conn, result));

	// This represents the LocalState output from this worker node; the coordinator combines the outputs of all
	// workers, mirroring how thread-local sink states are combined into a global state. The reader pulls batches from
	// the streaming result as Flight sends them.
	return QueryResultToArrowReader(std::move(task_conn), std::move(result), reader);
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
