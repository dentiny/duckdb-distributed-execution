#include "server/driver/distributed_flight_server.hpp"

#include "distributed_protocol.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "query_common.hpp"
#include "server/driver/duckling_storage.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

DistributedFlightServer::DistributedFlightServer(string host_p, int port_p) : host(std::move(host_p)), port(port_p) {
	Initialize();
	session_sweeper = std::thread(&DistributedFlightServer::SessionSweepLoop, this);
}

DistributedFlightServer::~DistributedFlightServer() {
	Shutdown();
	const std::unique_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
	DestroyState();
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
	StopSessionSweeper();
	if (shutdown_started.exchange(true)) {
		return;
	}
	const std::unique_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
	CloseAllSessions();
	auto status = FlightServerBase::Shutdown();
	// Ignore shutdown errors in production
}

void DistributedFlightServer::Reset() {
	const std::unique_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
	Initialize();
}

void DistributedFlightServer::Initialize() {
	DestroyState();
	// Clear query history.
	{
		const std::lock_guard<std::mutex> lock(query_history_mutex);
		query_history.clear();
	}

	// Register the Duckling storage extension.
	DBConfig config;
	StorageExtension::Register(config, "duckling", make_shared_ptr<DucklingStorageExtension>());

	db = make_uniq<DuckDB>(nullptr, &config);
	conn = make_uniq<Connection>(*db);

	// Attach duckling storage extension.
	auto result = conn->Query("ATTACH DATABASE ':memory:' AS duckling (TYPE duckling);");
	if (result->HasError()) {
		throw InternalException(StringUtil::Format("Failed to attach Duckling: %s", result->GetError()));
	}

	// Set duckling as the default database.
	auto use_result = conn->Query("USE duckling;");
	if (use_result->HasError()) {
		throw InternalException(StringUtil::Format("Failed to USE duckling: %s", use_result->GetError()));
	}

	// Initialize worker manager and distributed executor.
	worker_manager = make_uniq<WorkerManager>(*db);
	distributed_executor = make_uniq<DistributedExecutor>(*worker_manager, *conn);
}

void DistributedFlightServer::DestroyState() {
	CloseAllSessions();
	distributed_executor.reset();
	if (worker_manager) {
		worker_manager->Shutdown();
		worker_manager.reset();
	}
	conn.reset();
	db.reset();
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

arrow::Status DistributedFlightServer::DoActionImpl(const arrow::flight::ServerCallContext &context,
                                                    const arrow::flight::Action &action,
                                                    std::unique_ptr<arrow::flight::ResultStream> *result) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(action.body->data(), action.body->size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}

	distributed::DistributedResponse response;
	response.set_success(true);

	switch (request.request_case()) {
	// ========== Table perations ==========
	case distributed::DistributedRequest::kCreateTable:
		ARROW_RETURN_NOT_OK(HandleCreateTable(request.create_table(), response));
		break;
	case distributed::DistributedRequest::kDropTable:
		ARROW_RETURN_NOT_OK(HandleDropTable(request.drop_table(), response));
		break;
	case distributed::DistributedRequest::kAlterTable:
		ARROW_RETURN_NOT_OK(HandleAlterTable(request.alter_table(), response));
		break;

	// ========== Index perations ==========
	case distributed::DistributedRequest::kCreateIndex:
		ARROW_RETURN_NOT_OK(HandleCreateIndex(request.create_index(), response));
		break;
	case distributed::DistributedRequest::kDropIndex:
		ARROW_RETURN_NOT_OK(HandleDropIndex(request.drop_index(), response));
		break;

	// ========== Query & Utility Operations ==========
	case distributed::DistributedRequest::kExecuteSql:
		ARROW_RETURN_NOT_OK(HandleExecuteSQL(request.execute_sql(), response));
		break;
	case distributed::DistributedRequest::kSessionOpen:
		ARROW_RETURN_NOT_OK(HandleSessionOpen(request.session_open(), response));
		break;
	case distributed::DistributedRequest::kSessionClose:
		ARROW_RETURN_NOT_OK(HandleSessionClose(request.session_close(), response));
		break;
	case distributed::DistributedRequest::kTableExists:
		ARROW_RETURN_NOT_OK(HandleTableExists(request.table_exists(), response));
		break;
	case distributed::DistributedRequest::kLoadExtension:
		ARROW_RETURN_NOT_OK(HandleLoadExtension(request.load_extension(), response));
		break;

	// ========== Stats & Monitoring Operations ==========
	case distributed::DistributedRequest::kGetQueryExecutionStats:
		ARROW_RETURN_NOT_OK(HandleGetQueryExecutionStats(request.get_query_execution_stats(), response));
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
	const std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
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

	std::unique_ptr<arrow::flight::FlightDataStream> data_stream;
	ARROW_RETURN_NOT_OK(HandleScanTable(request.scan_table(), data_stream));

	*stream = std::move(data_stream);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoGet(const arrow::flight::ServerCallContext &context,
                                             const arrow::flight::Ticket &ticket,
                                             std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
	const std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
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
	std::string table_name;
	if (!descriptor.path.empty()) {
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

		ARROW_RETURN_NOT_OK(HandleInsertData(table_name, batch, resp));
	}

	// Write response metadata.
	std::string resp_data = resp.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(resp_data);
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoPut(const arrow::flight::ServerCallContext &context,
                                             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                                             std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {
	const std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
	try {
		return DoPutImpl(context, std::move(reader), std::move(writer));
	} catch (const std::exception &e) {
		std::cerr << "[FATAL] DoPut exception: " << e.what() << std::endl;
		return arrow::Status::UnknownError(StringUtil::Format("DoPut exception: %s", e.what()));
	}
}

shared_ptr<DistributedFlightServer::FlightSession> DistributedFlightServer::GetSession(const string &session_id) {
	const std::lock_guard<std::mutex> lock(sessions_mutex);
	auto entry = sessions.find(session_id);
	if (entry == sessions.end()) {
		return nullptr;
	}
	return entry->second;
}

bool DistributedFlightServer::ValidateProtocol(uint32_t version, uint64_t required_capabilities, string &error) {
	if (version != DUCKHERDER_PROTOCOL_VERSION) {
		error = StringUtil::Format("Unsupported Duckherder protocol version %u (server requires %u)", version,
		                           DUCKHERDER_PROTOCOL_VERSION);
		return false;
	}
	if ((required_capabilities & DUCKHERDER_REQUIRED_CAPABILITIES) != required_capabilities) {
		error = "Server does not support the requested Duckherder protocol capabilities";
		return false;
	}
	return true;
}

void DistributedFlightServer::SetSessionTimeoutForTesting(std::chrono::milliseconds timeout) {
	const std::lock_guard<std::mutex> lock(sessions_mutex);
	session_timeout = timeout;
	session_sweeper_cv.notify_all();
}

void DistributedFlightServer::FailNextCommitResponseForTesting() {
	FailCommitResponsesForTesting(1);
}

void DistributedFlightServer::FailCommitResponsesForTesting(uint32_t count) {
	fail_commit_responses = count;
}

bool DistributedFlightServer::ShouldFailCommitResponseForTesting() {
	auto remaining = fail_commit_responses.load();
	while (remaining > 0 && !fail_commit_responses.compare_exchange_weak(remaining, remaining - 1)) {
	}
	return remaining > 0;
}

void DistributedFlightServer::SessionSweepLoop() {
	std::unique_lock<std::mutex> lock(session_sweeper_mutex);
	while (!stop_session_sweeper) {
		session_sweeper_cv.wait_for(lock, std::chrono::milliseconds(100));
		if (stop_session_sweeper) {
			break;
		}
		lock.unlock();
		{
			const std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex);
			SweepExpiredSessions();
		}
		lock.lock();
	}
}

void DistributedFlightServer::StopSessionSweeper() {
	if (stop_session_sweeper.exchange(true)) {
		return;
	}
	session_sweeper_cv.notify_all();
	if (session_sweeper.joinable()) {
		session_sweeper.join();
	}
}

void DistributedFlightServer::SweepExpiredSessions() {
	vector<shared_ptr<FlightSession>> expired;
	auto now = std::chrono::steady_clock::now();
	{
		const std::lock_guard<std::mutex> sessions_lock(sessions_mutex);
		for (auto entry = sessions.begin(); entry != sessions.end();) {
			auto &session = *entry->second;
			const std::lock_guard<std::mutex> session_lock(session.mutex);
			if (!session.closing && now - session.last_used < session_timeout) {
				++entry;
				continue;
			}
			session.closing = true;
			expired.push_back(entry->second);
			entry = sessions.erase(entry);
		}
	}
	for (auto &session : expired) {
		const std::lock_guard<std::mutex> lock(session->mutex);
		if (session->connection->HasActiveTransaction()) {
			session->connection->Query("ROLLBACK");
		}
	}
}

void DistributedFlightServer::CloseAllSessions() {
	unordered_map<string, shared_ptr<FlightSession>> sessions_to_close;
	{
		const std::lock_guard<std::mutex> lock(sessions_mutex);
		sessions_to_close.swap(sessions);
	}
	for (auto &entry : sessions_to_close) {
		auto &session = *entry.second;
		const std::lock_guard<std::mutex> lock(session.mutex);
		session.closing = true;
		if (session.connection->HasActiveTransaction()) {
			session.connection->Query("ROLLBACK");
		}
	}
}

arrow::Status DistributedFlightServer::HandleSessionOpen(const distributed::SessionOpenRequest &req,
                                                         distributed::DistributedResponse &resp) {
	string protocol_error;
	if (!ValidateProtocol(req.protocol_version(), req.required_capabilities(), protocol_error)) {
		resp.set_success(false);
		resp.set_error_message(protocol_error);
		return arrow::Status::OK();
	}
	if (req.session_id().empty()) {
		resp.set_success(false);
		resp.set_error_message("Session identifier must not be empty");
		return arrow::Status::OK();
	}
	SweepExpiredSessions();
	if (auto existing = GetSession(req.session_id())) {
		const std::lock_guard<std::mutex> lock(existing->mutex);
		if (!existing->closing) {
			existing->last_used = std::chrono::steady_clock::now();
			resp.set_success(true);
			auto *open_response = resp.mutable_session_open();
			open_response->set_session_id(req.session_id());
			open_response->set_protocol_version(DUCKHERDER_PROTOCOL_VERSION);
			open_response->set_capabilities(DUCKHERDER_REQUIRED_CAPABILITIES);
			return arrow::Status::OK();
		}
	}

	auto session_connection = make_uniq<Connection>(*db);
	auto use_result = session_connection->Query("USE duckling");
	if (use_result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(use_result->GetError());
		return arrow::Status::OK();
	}

	auto session = make_shared_ptr<FlightSession>(std::move(session_connection));
	{
		const std::lock_guard<std::mutex> lock(sessions_mutex);
		auto inserted = sessions.emplace(req.session_id(), session);
		if (!inserted.second) {
			session = inserted.first->second;
		}
	}

	resp.set_success(true);
	auto *open_response = resp.mutable_session_open();
	open_response->set_session_id(req.session_id());
	open_response->set_protocol_version(DUCKHERDER_PROTOCOL_VERSION);
	open_response->set_capabilities(DUCKHERDER_REQUIRED_CAPABILITIES);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleSessionClose(const distributed::SessionCloseRequest &req,
                                                          distributed::DistributedResponse &resp) {
	string protocol_error;
	if (!ValidateProtocol(req.protocol_version(), DUCKHERDER_CAPABILITY_SESSIONS, protocol_error)) {
		resp.set_success(false);
		resp.set_error_message(protocol_error);
		return arrow::Status::OK();
	}
	SweepExpiredSessions();
	shared_ptr<FlightSession> session;
	{
		const std::lock_guard<std::mutex> lock(sessions_mutex);
		auto entry = sessions.find(req.session_id());
		if (entry == sessions.end()) {
			// Close is deliberately idempotent so a lost response can be retried.
			resp.set_success(true);
			resp.mutable_session_close();
			return arrow::Status::OK();
		}
		session = entry->second;
		sessions.erase(entry);
	}

	const std::lock_guard<std::mutex> lock(session->mutex);
	session->closing = true;
	if (session->connection->HasActiveTransaction()) {
		auto rollback_result = session->connection->Query("ROLLBACK");
		if (rollback_result->HasError()) {
			resp.set_success(false);
			resp.set_error_message(rollback_result->GetError());
			return arrow::Status::OK();
		}
	}
	resp.set_success(true);
	resp.mutable_session_close();
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleExecuteSQL(const distributed::ExecuteSQLRequest &req,
                                                        distributed::DistributedResponse &resp) {
	if (req.session_id().empty()) {
		const std::lock_guard<std::mutex> lock(connection_mutex);
		return ExecuteSQLOnConnection(*conn, req.sql(), resp, true);
	}

	string protocol_error;
	if (!ValidateProtocol(req.protocol_version(), DUCKHERDER_CAPABILITY_SESSIONS, protocol_error)) {
		resp.set_success(false);
		resp.set_error_message(protocol_error);
		return arrow::Status::OK();
	}
	SweepExpiredSessions();
	auto session = GetSession(req.session_id());
	if (!session) {
		resp.set_success(false);
		resp.set_error_message("Unknown session");
		return arrow::Status::OK();
	}
	const std::lock_guard<std::mutex> lock(session->mutex);
	if (session->closing) {
		resp.set_success(false);
		resp.set_error_message("Session is closed");
		return arrow::Status::OK();
	}
	session->last_used = std::chrono::steady_clock::now();
	const bool is_begin = StringUtil::CIEquals(req.sql(), "BEGIN TRANSACTION");
	const bool is_commit = StringUtil::CIEquals(req.sql(), "COMMIT");
	const bool is_rollback = StringUtil::CIEquals(req.sql(), "ROLLBACK");
	if ((is_begin && session->transaction_status == SessionTransactionStatus::ACTIVE) ||
	    (is_commit && session->transaction_status == SessionTransactionStatus::COMMITTED) ||
	    (is_rollback && session->transaction_status == SessionTransactionStatus::ROLLED_BACK)) {
		resp.set_success(true);
		resp.mutable_execute_sql()->set_rows_affected(0);
		if (is_commit && ShouldFailCommitResponseForTesting()) {
			return arrow::Status::IOError("Injected lost COMMIT response");
		}
		return arrow::Status::OK();
	}
	// Session statements are deliberately kept on the control connection. In particular,
	// uncommitted state is not visible to worker connections.
	auto status = ExecuteSQLOnConnection(*session->connection, req.sql(), resp, false);
	if (!status.ok() || !resp.success()) {
		return status;
	}
	if (is_begin) {
		session->transaction_status = SessionTransactionStatus::ACTIVE;
	} else if (is_commit) {
		session->transaction_status = SessionTransactionStatus::COMMITTED;
	} else if (is_rollback) {
		session->transaction_status = SessionTransactionStatus::ROLLED_BACK;
	}
	if (is_commit && ShouldFailCommitResponseForTesting()) {
		return arrow::Status::IOError("Injected lost COMMIT response");
	}
	return status;
}

arrow::Status DistributedFlightServer::ExecuteSQLOnConnection(Connection &connection, const string &sql,
                                                              distributed::DistributedResponse &resp,
                                                              bool allow_workers) {
	// Start tracking query execution
	QueryExecutionInfo query_info;
	query_info.sql = sql;
	auto query_start = std::chrono::steady_clock::now();
	query_info.execution_start_time = std::chrono::system_clock::now();

	// Try distributed execution first if workers are available.
	unique_ptr<QueryResult> result;
	if (allow_workers && worker_manager != nullptr && worker_manager->GetWorkerCount() > 0) {
		auto exec_result = distributed_executor->ExecuteDistributed(sql);

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
		result = connection.Query(sql);
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
	int64_t rows_affected = 0;
	if (result->ColumnCount() == 1 && result->RowCount() == 1) {
		auto count = result->GetValue(0, 0);
		if (!count.IsNull() && count.type() == LogicalType::BIGINT) {
			rows_affected = count.GetValue<int64_t>();
		}
	}
	exec_resp->set_rows_affected(rows_affected);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleCreateTable(const distributed::CreateTableRequest &req,
                                                         distributed::DistributedResponse &resp) {
	auto result = conn->Query(req.sql());

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
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP TABLE IF EXISTS " + req.table_name();
	auto result = conn->Query(sql);

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
                                                         distributed::DistributedResponse &resp) {
	auto result = conn->Query(req.sql());

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
                                                       distributed::DistributedResponse &resp) {
	auto sql = "DROP INDEX IF EXISTS " + req.index_name();
	auto result = conn->Query(sql);

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
                                                        distributed::DistributedResponse &resp) {
	auto result = conn->Query(req.sql());

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
	auto install_result = conn->Query(sql);
	if (install_result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(
		    StringUtil::Format("Extension %s install failed %s", req.extension_name(), install_result->GetError()));
		return arrow::Status::OK();
	}

	// Then LOAD the extension.
	sql = "LOAD " + req.extension_name();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Load extension with %s", sql));
	auto load_result = conn->Query(sql);
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
                                                         distributed::DistributedResponse &resp) {
	const std::lock_guard<std::mutex> lock(connection_mutex);
	string sql =
	    StringUtil::Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.table_name());

	auto result = conn->Query(sql);

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
                                                       std::unique_ptr<arrow::flight::FlightDataStream> &stream) {
	string protocol_error;
	if (!ValidateProtocol(req.protocol_version(), DUCKHERDER_CAPABILITY_STRUCTURAL_SCAN, protocol_error)) {
		return arrow::Status::Invalid(protocol_error);
	}
	if (req.session_id().empty()) {
		const std::lock_guard<std::mutex> lock(connection_mutex);
		return HandleScanTableOnConnection(*conn, req, stream, true);
	}

	SweepExpiredSessions();
	auto session = GetSession(req.session_id());
	if (!session) {
		return arrow::Status::Invalid("Unknown session");
	}
	const std::lock_guard<std::mutex> lock(session->mutex);
	if (session->closing) {
		return arrow::Status::Invalid("Session is closed");
	}
	session->last_used = std::chrono::steady_clock::now();
	return HandleScanTableOnConnection(*session->connection, req, stream, false);
}

arrow::Status
DistributedFlightServer::HandleScanTableOnConnection(Connection &connection, const distributed::ScanTableRequest &req,
                                                     std::unique_ptr<arrow::flight::FlightDataStream> &stream,
                                                     bool allow_workers) {
	auto &db_instance = *db->instance.get();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Handling scan for table: %s", req.table_name()));

	// The scan protocol accepts a structural table identifier, never an arbitrary query string.
	auto table_identifier = QualifiedName::Parse(req.table_name()).ToString();
	vector<string> select_list;
	if (req.include_rowid()) {
		select_list.emplace_back("rowid");
	}
	if (req.project_columns()) {
		for (auto &column : req.projected_columns()) {
			select_list.push_back(KeywordHelper::WriteOptionallyQuoted(column));
		}
		if (select_list.empty()) {
			return arrow::Status::Invalid("Projected scan must request at least one column");
		}
	} else {
		select_list.emplace_back("*");
	}
	string sql = StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), table_identifier);

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
	if (allow_workers && worker_manager != nullptr && worker_manager->GetWorkerCount() > 0) {
		auto exec_result = distributed_executor->ExecuteDistributed(sql);

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
		result = connection.Query(sql);
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
		result->client_properties.client_context = connection.context.get();
	}

	std::shared_ptr<arrow::RecordBatchReader> reader;
	ARROW_RETURN_NOT_OK(QueryResultToArrow(*result, reader));

	stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::HandleInsertData(const std::string &table_name,
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

	auto result = conn->Query(insert_sql);
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
	ArrowConverter::ToArrowSchema(&arrow_schema, result.GetTypes(), IdentifiersToStrings(result.GetNames()),
	                              result.client_properties);
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
		    ArrowTypeExtensionData::GetExtensionTypes(*result.client_properties.client_context, result.GetTypes());
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
	const std::lock_guard<std::mutex> lock(query_history_mutex);
	query_history.emplace_back(info);
}

vector<QueryExecutionInfo> DistributedFlightServer::GetQueryExecutions() const {
	const std::lock_guard<std::mutex> lock(query_history_mutex);
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
