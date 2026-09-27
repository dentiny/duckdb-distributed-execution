#include "server/driver/distributed_flight_server.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "query_common.hpp"
#include "server/driver/duckling_storage.hpp"
#include "utils/time_utils.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

DistributedFlightServer::ClientRegistration::ClientRegistration(DuckDB &db, WorkerManager &worker_manager,
                                                                distributed::ClientRole role_p)
    : role(role_p), last_seen(GetSteadyNowMilliSecSinceEpoch()), connection(make_uniq<Connection>(db)) {
	auto use_result = connection->Query("USE duckling;");
	if (use_result->HasError()) {
		throw InternalException(
		    StringUtil::Format("Failed to USE duckling for client connection: %s", use_result->GetError()));
	}
	distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection);
}

DistributedFlightServer::DistributedFlightServer(string host_p, int port_p) : host(std::move(host_p)), port(port_p) {
	Initialize();
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

void DistributedFlightServer::Reset() {
	const unique_lock<std::shared_mutex> lock(clients_mutex);
	clients.clear();
	writable_client_id.clear();
	Initialize();
}

void DistributedFlightServer::Initialize() {
	// Release objects that reference the previous database in dependency order.
	worker_manager.reset();
	db.reset();

	// Clear query history.
	{
		const lock_guard<mutex> lock(query_history_mutex);
		query_history.clear();
	}

	// Register the Duckling storage extension.
	DBConfig config;
	StorageExtension::Register(config, "duckling", make_shared_ptr<DucklingStorageExtension>());

	db = make_uniq<DuckDB>(nullptr, &config);
	Connection bootstrap_conn(*db);

	// Attach duckling storage extension.
	auto result = bootstrap_conn.Query("ATTACH DATABASE ':memory:' AS duckling (TYPE duckling);");
	if (result->HasError()) {
		throw InternalException(StringUtil::Format("Failed to attach Duckling: %s", result->GetError()));
	}

	// Initialize the worker manager. Each client registration owns its connection-bound executor.
	worker_manager = make_uniq<WorkerManager>(*db);
}

string DistributedFlightServer::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host, port);
}

void DistributedFlightServer::RegisterWorker(const string &worker_id, const string &location) {
	if (!worker_manager) {
		throw InternalException("WorkerManager not initialized");
	}
	worker_manager->RegisterWorker(worker_id, location);
}

void DistributedFlightServer::RegisterOrReplaceDriver(const string &driver_id, const string &location) {
	if (!worker_manager) {
		throw InternalException("WorkerManager not initialized");
	}
	worker_manager->RegisterOrReplaceDriver(driver_id, location);
}

idx_t DistributedFlightServer::GetWorkerCount() const {
	if (!worker_manager) {
		return 0;
	}
	return worker_manager->GetWorkerCount();
}

void DistributedFlightServer::StartLocalWorkers(idx_t num_workers) {
	if (!worker_manager) {
		throw InternalException("WorkerManager not initialized");
	}
	worker_manager->StartLocalWorkers(num_workers);
}

void DistributedFlightServer::SetClientLeaseTimeoutForTesting(std::chrono::milliseconds timeout) {
	const unique_lock<std::shared_mutex> lock(clients_mutex);
	client_lease_timeout = timeout;
}

bool DistributedFlightServer::LookupClient(const string &client_id, shared_ptr<ClientRegistration> &registration) {
	auto entry = clients.find(client_id);
	if (entry == clients.end()) {
		return false;
	}
	registration = entry->second;
	return true;
}

void DistributedFlightServer::TouchClient(const shared_ptr<ClientRegistration> &registration) {
	registration->last_seen = GetSteadyNowMilliSecSinceEpoch();
}

void DistributedFlightServer::PruneExpiredClients() {
	const auto expiration = GetSteadyNowMilliSecSinceEpoch() - client_lease_timeout.count();
	for (auto entry = clients.begin(); entry != clients.end();) {
		if (entry->second->last_seen.load() >= expiration) {
			++entry;
			continue;
		}
		if (writable_client_id == entry->first) {
			writable_client_id.clear();
		}
		entry = clients.erase(entry);
	}
}

bool DistributedFlightServer::AuthorizeClient(const string &client_id, distributed::ClientRole required_role,
                                              shared_ptr<ClientRegistration> &registration,
                                              distributed::DistributedResponse &resp) {
	if (!LookupClient(client_id, registration)) {
		resp.set_success(false);
		resp.set_error_message("Duckherder client is not registered with the control node");
		return false;
	}
	if (required_role == distributed::CLIENT_ROLE_READ_WRITE &&
	    registration->role != distributed::CLIENT_ROLE_READ_WRITE) {
		resp.set_success(false);
		resp.set_error_message("Duckherder client is read-only");
		return false;
	}
	TouchClient(registration);
	return true;
}

arrow::Status DistributedFlightServer::HandleRegisterClient(const distributed::RegisterClientRequest &req,
                                                            distributed::DistributedResponse &resp) {
	const unique_lock<std::shared_mutex> lock(clients_mutex);
	PruneExpiredClients();
	if (req.role() != distributed::CLIENT_ROLE_READ_ONLY && req.role() != distributed::CLIENT_ROLE_READ_WRITE) {
		resp.set_success(false);
		resp.set_error_message("Duckherder client role must be specified");
		return arrow::Status::OK();
	}
	if (req.role() == distributed::CLIENT_ROLE_READ_WRITE && !writable_client_id.empty()) {
		resp.set_success(false);
		resp.set_error_message("Control node already has a writable Duckherder client");
		return arrow::Status::OK();
	}

	auto client_id = UUID::ToString(UUID::GenerateRandomUUID());
	clients.emplace(client_id, make_shared_ptr<ClientRegistration>(*db, *worker_manager, req.role()));
	if (req.role() == distributed::CLIENT_ROLE_READ_WRITE) {
		writable_client_id = client_id;
	}
	resp.set_success(true);
	resp.mutable_register_client()->set_client_id(client_id);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleUnregisterClient(const string &client_id,
                                                              distributed::DistributedResponse &resp) {
	const unique_lock<std::shared_mutex> lock(clients_mutex);
	auto entry = clients.find(client_id);
	if (entry != clients.end()) {
		if (writable_client_id == client_id) {
			writable_client_id.clear();
		}
		clients.erase(entry);
	}
	resp.set_success(true);
	resp.mutable_unregister_client();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoActionImpl(const arrow::flight::ServerCallContext &context,
                                                    const arrow::flight::Action &action,
                                                    std::unique_ptr<arrow::flight::ResultStream> *result) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(action.body->data(), action.body->size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	distributed::DistributedResponse response;
	response.set_success(true);

	std::shared_lock<std::shared_mutex> client_lock;
	if (request.request_case() != distributed::DistributedRequest::kRegisterClient &&
	    request.request_case() != distributed::DistributedRequest::kUnregisterClient) {
		client_lock = std::shared_lock<std::shared_mutex>(clients_mutex);
	}
	shared_ptr<ClientRegistration> registration;

	switch (request.request_case()) {
	case distributed::DistributedRequest::kRegisterClient:
		ARROW_RETURN_NOT_OK(HandleRegisterClient(request.register_client(), response));
		break;
	case distributed::DistributedRequest::kUnregisterClient:
		ARROW_RETURN_NOT_OK(HandleUnregisterClient(request.client_id(), response));
		break;
	// ========== Table perations ==========
	case distributed::DistributedRequest::kCreateTable:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleCreateTable(request.create_table(), *registration, response));
		}
		break;
	case distributed::DistributedRequest::kDropTable:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleDropTable(request.drop_table(), *registration, response));
		}
		break;
	case distributed::DistributedRequest::kAlterTable:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleAlterTable(request.alter_table(), *registration, response));
		}
		break;

	// ========== Index perations ==========
	case distributed::DistributedRequest::kCreateIndex:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleCreateIndex(request.create_index(), *registration, response));
		}
		break;
	case distributed::DistributedRequest::kDropIndex:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleDropIndex(request.drop_index(), *registration, response));
		}
		break;

	// ========== Query & Utility Operations ==========
	case distributed::DistributedRequest::kExecuteSql:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleExecuteSQL(request.execute_sql(), *registration, response));
		}
		break;
	case distributed::DistributedRequest::kTableExists:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleTableExists(request.table_exists(), *registration, response));
		}
		break;
	case distributed::DistributedRequest::kLoadExtension:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleLoadExtension(request.load_extension(), *registration, response));
		}
		break;

	// ========== Stats & Monitoring Operations ==========
	case distributed::DistributedRequest::kGetQueryExecutionStats:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			ARROW_RETURN_NOT_OK(HandleGetQueryExecutionStats(request.get_query_execution_stats(), response));
		}
		break;
	case distributed::DistributedRequest::kClientHeartbeat:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			response.mutable_client_heartbeat();
		}
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

arrow::Status DistributedFlightServer::DoAction(const arrow::flight::ServerCallContext &context,
                                                const arrow::flight::Action &action,
                                                std::unique_ptr<arrow::flight::ResultStream> *result) {
	try {
		return DoActionImpl(context, action, result);
	} catch (const std::exception &e) {
		std::cerr << "[FATAL] DoAction exception: " << e.what() << std::endl;
		return arrow::Status::UnknownError(StringUtil::Format("DoAction exception: %s", e.what()));
	}
}

arrow::Status DistributedFlightServer::DoGetImpl(const arrow::flight::ServerCallContext &context,
                                                 const arrow::flight::Ticket &ticket,
                                                 std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(ticket.ticket.data(), ticket.ticket.size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	if (request.request_case() != distributed::DistributedRequest::kScanTable) {
		return arrow::Status::Invalid("DoGet only supports SCAN_TABLE requests");
	}
	const std::shared_lock<std::shared_mutex> client_lock(clients_mutex);
	shared_ptr<ClientRegistration> registration;
	if (!LookupClient(request.client_id(), registration)) {
		return arrow::Status::Invalid("Duckherder client is not registered with the control node");
	}
	TouchClient(registration);

	const lock_guard<mutex> connection_lock(registration->connection_mutex);
	std::unique_ptr<arrow::flight::FlightDataStream> data_stream;
	ARROW_RETURN_NOT_OK(HandleScanTable(request.scan_table(), *registration, data_stream));

	TouchClient(registration);
	*stream = std::move(data_stream);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoGet(const arrow::flight::ServerCallContext &context,
                                             const arrow::flight::Ticket &ticket,
                                             std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
	try {
		return DoGetImpl(context, ticket, stream);
	} catch (const std::exception &e) {
		std::cerr << "[FATAL] DoGet exception: " << e.what() << std::endl;
		return arrow::Status::UnknownError(StringUtil::Format("DoGet exception: %s", e.what()));
	}
}

arrow::Status DistributedFlightServer::DoPutImpl(const arrow::flight::ServerCallContext &context,
                                                 std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                                                 std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {
	auto descriptor = reader->descriptor();
	if (descriptor.path.size() < 2) {
		return arrow::Status::Invalid("DoPut requires a registered client and table name");
	}
	const auto &client_id = descriptor.path[0];
	const std::shared_lock<std::shared_mutex> client_lock(clients_mutex);
	shared_ptr<ClientRegistration> registration;
	if (!LookupClient(client_id, registration)) {
		return arrow::Status::Invalid("Duckherder client is not registered with the control node");
	}
	if (registration->role != distributed::CLIENT_ROLE_READ_WRITE) {
		return arrow::Status::Invalid("Duckherder client is read-only");
	}
	TouchClient(registration);

	const lock_guard<mutex> connection_lock(registration->connection_mutex);
	std::string table_name;
	table_name = descriptor.path[1];

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

		ARROW_RETURN_NOT_OK(HandleInsertData(table_name, batch, *registration, resp));
	}

	// Write response metadata.
	std::string resp_data = resp.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(resp_data);
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	TouchClient(registration);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoPut(const arrow::flight::ServerCallContext &context,
                                             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                                             std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {
	try {
		return DoPutImpl(context, std::move(reader), std::move(writer));
	} catch (const std::exception &e) {
		std::cerr << "[FATAL] DoPut exception: " << e.what() << std::endl;
		return arrow::Status::UnknownError(StringUtil::Format("DoPut exception: %s", e.what()));
	}
}

arrow::Status DistributedFlightServer::HandleExecuteSQL(const distributed::ExecuteSQLRequest &req,
                                                        ClientRegistration &registration,
                                                        distributed::DistributedResponse &resp) {
	// Start tracking query execution
	QueryExecutionInfo query_info;
	query_info.sql = req.sql();
	auto query_start = std::chrono::steady_clock::now();
	query_info.execution_start_time = std::chrono::system_clock::now();

	// Try distributed execution first if workers are available.
	unique_ptr<QueryResult> result;
	if (worker_manager != nullptr && worker_manager->GetWorkerCount() > 0) {
		auto exec_result = registration.distributed_executor->ExecuteDistributed(req.sql());

		if (exec_result.result != nullptr) {
			// Query was executed in distributed mode
			result = std::move(exec_result.result);
			query_info.num_workers_used = exec_result.num_workers_used;
			query_info.num_tasks_generated = exec_result.num_tasks;

			switch (exec_result.partition_strategy) {
			case PartitionStrategy::NONE:
				query_info.execution_mode = QueryExecutionMode::DELEGATED;
				break;
			case PartitionStrategy::ROW_GROUP_ALIGNED:
				query_info.execution_mode = QueryExecutionMode::ROW_GROUP_PARTITION;
				break;
			case PartitionStrategy::NATURAL:
				query_info.execution_mode = QueryExecutionMode::NATURAL_PARTITION;
				break;
			}
			query_info.merge_strategy = exec_result.merge_strategy;
		}
	}

	// Fall back to local execution if not distributed.
	if (result == nullptr) {
		result = registration.connection->Query(req.sql());
		// Mark as local execution for non-distributed queries
		query_info.execution_mode = QueryExecutionMode::LOCAL;
		query_info.num_workers_used = 0;
		query_info.num_tasks_generated = 0;
	}

	auto query_end = std::chrono::steady_clock::now();
	query_info.query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start);

	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}

	// Record all successful query executions.
	RecordQueryExecution(std::move(query_info));

	resp.set_success(true);
	auto *exec_resp = resp.mutable_execute_sql();
	exec_resp->set_rows_affected(0);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateTable(const distributed::CreateTableRequest &req,
                                                         ClientRegistration &registration,
                                                         distributed::DistributedResponse &resp) {
	auto result = registration.connection->Query(req.sql());

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
                                                       ClientRegistration &registration,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP TABLE IF EXISTS " + req.table_name();
	auto result = registration.connection->Query(sql);

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
                                                         ClientRegistration &registration,
                                                         distributed::DistributedResponse &resp) {
	auto result = registration.connection->Query(req.sql());

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
                                                       ClientRegistration &registration,
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP INDEX IF EXISTS " + req.index_name();
	auto result = registration.connection->Query(sql);

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
                                                        ClientRegistration &registration,
                                                        distributed::DistributedResponse &resp) {
	auto result = registration.connection->Query(req.sql());

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
                                                           ClientRegistration &registration,
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

arrow::Status DistributedFlightServer::HandleTableExists(const distributed::TableExistsRequest &req,
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

arrow::Status DistributedFlightServer::HandleScanTable(const distributed::ScanTableRequest &req,
                                                       ClientRegistration &registration,
                                                       std::unique_ptr<arrow::flight::FlightDataStream> &stream) {
	auto &db_instance = *db->instance.get();
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

	if (req.limit() != NO_QUERY_LIMIT && req.limit() != STANDARD_VECTOR_SIZE) {
		sql += StringUtil::Format(" LIMIT %llu ", req.limit());
	}
	if (req.offset() != NO_QUERY_OFFSET) {
		sql += StringUtil::Format(" OFFSET %llu ", req.offset());
	}

	// Start tracking query execution
	QueryExecutionInfo query_info;
	query_info.sql = sql;
	auto query_start = std::chrono::steady_clock::now();                // For duration calculation
	query_info.execution_start_time = std::chrono::system_clock::now(); // Wall-clock timestamp

	// Try distributed execution first if workers are available.
	unique_ptr<QueryResult> result;
	if (worker_manager != nullptr && worker_manager->GetWorkerCount() > 0) {
		auto exec_result = registration.distributed_executor->ExecuteDistributed(sql);

		if (exec_result.result != nullptr) {
			// Query was executed in distributed mode
			result = std::move(exec_result.result);
			query_info.num_workers_used = exec_result.num_workers_used;
			query_info.num_tasks_generated = exec_result.num_tasks;

			// Map partition strategy to execution mode
			switch (exec_result.partition_strategy) {
			case PartitionStrategy::NONE:
				query_info.execution_mode = QueryExecutionMode::DELEGATED;
				break;
			case PartitionStrategy::ROW_GROUP_ALIGNED:
				query_info.execution_mode = QueryExecutionMode::ROW_GROUP_PARTITION;
				break;
			case PartitionStrategy::NATURAL:
				query_info.execution_mode = QueryExecutionMode::NATURAL_PARTITION;
				break;
			}
			query_info.merge_strategy = exec_result.merge_strategy;
		}
	}

	// Fall back to local execution if not distributed.
	if (result == nullptr) {
		result = registration.connection->Query(sql);
		query_info.execution_mode = QueryExecutionMode::LOCAL;
		query_info.num_workers_used = 0;
		query_info.num_tasks_generated = 0;
	}

	// Calculate total query duration (using steady_clock for accurate elapsed time)
	auto query_end = std::chrono::steady_clock::now();
	query_info.query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start);

	// Record all successful query executions (both distributed and local)
	RecordQueryExecution(std::move(query_info));

	if (result->HasError()) {
		return arrow::Status::Invalid("Query error: " + result->GetError());
	}

	if (!result->client_properties.client_context) {
		result->client_properties.client_context = registration.connection->context.get();
	}

	std::shared_ptr<arrow::RecordBatchReader> reader;
	ARROW_RETURN_NOT_OK(QueryResultToArrow(*result, reader));

	stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleInsertData(const std::string &table_name,
                                                        std::shared_ptr<arrow::RecordBatch> batch,
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

arrow::Status DistributedFlightServer::QueryResultToArrow(QueryResult &result,
                                                          std::shared_ptr<arrow::RecordBatchReader> &reader,
                                                          idx_t *row_count) {
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

		ArrowArray arrow_array;
		auto extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(*result.client_properties.client_context, result.types);
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, result.client_properties, extension_types);

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
	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(std::move(batches), std::move(schema)));
	if (row_count) {
		*row_count = count;
	}

	return arrow::Status::OK();
}

void DistributedFlightServer::RecordQueryExecution(QueryExecutionInfo info) {
	const lock_guard<mutex> lock(query_history_mutex);
	query_history.emplace_back(info);
}

vector<QueryExecutionInfo> DistributedFlightServer::GetQueryExecutions() const {
	const lock_guard<mutex> lock(query_history_mutex);
	return query_history;
}

arrow::Status
DistributedFlightServer::HandleGetQueryExecutionStats(const distributed::GetQueryExecutionStatsRequest &req,
                                                      distributed::DistributedResponse &resp) {
	auto query_executions = GetQueryExecutions();
	resp.set_success(true);
	auto *stats_resp = resp.mutable_get_query_execution_stats();

	for (const auto &exec_info : query_executions) {
		auto *query_info = stats_resp->add_query_executions();
		query_info->set_sql(exec_info.sql);

		switch (exec_info.execution_mode) {
		case QueryExecutionMode::LOCAL:
			query_info->set_execution_mode("LOCAL");
			break;
		case QueryExecutionMode::DELEGATED:
			query_info->set_execution_mode("DELEGATED");
			break;
		case QueryExecutionMode::NATURAL_PARTITION:
			query_info->set_execution_mode("NATURAL_PARTITION");
			break;
		case QueryExecutionMode::ROW_GROUP_PARTITION:
			query_info->set_execution_mode("ROW_GROUP_PARTITION");
			break;
		}

		switch (exec_info.merge_strategy) {
		case QueryPlanAnalyzer::MergeStrategy::CONCATENATE:
			query_info->set_merge_strategy("CONCATENATE");
			break;
		case QueryPlanAnalyzer::MergeStrategy::AGGREGATE_MERGE:
			query_info->set_merge_strategy("AGGREGATE");
			break;
		case QueryPlanAnalyzer::MergeStrategy::GROUP_BY_MERGE:
			query_info->set_merge_strategy("GROUP_BY");
			break;
		case QueryPlanAnalyzer::MergeStrategy::DISTINCT_MERGE:
			query_info->set_merge_strategy("DISTINCT");
			break;
		}

		query_info->set_query_duration_ms(exec_info.query_duration.count());
		query_info->set_num_workers_used(exec_info.num_workers_used);
		query_info->set_num_tasks_generated(exec_info.num_tasks_generated);

		auto time_since_epoch = exec_info.execution_start_time.time_since_epoch();
		auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(time_since_epoch).count();
		query_info->set_execution_start_time_ms(milliseconds);
	}

	return arrow::Status::OK();
}

} // namespace duckdb
