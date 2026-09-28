#include "server/driver/distributed_flight_server.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "query_common.hpp"
#include "server/driver/duckling_storage.hpp"
#include "server/validation.hpp"
#include "transaction_constants.hpp"
#include "utils/time_utils.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace duckdb {

namespace {

idx_t TokenEnd(const string &sql, const vector<SimplifiedToken> &tokens, idx_t index) {
	auto end = index + 1 < tokens.size() ? tokens[index + 1].start : sql.size();
	while (end > tokens[index].start && StringUtil::CharacterIsSpace(sql[end - 1])) {
		end--;
	}
	return end;
}

string StripClientCatalog(const string &sql, const string &client_catalog) {
	if (client_catalog.empty()) {
		return sql;
	}

	auto tokens = Parser::Tokenize(sql);
	auto quoted_catalog = KeywordHelper::WriteQuoted(client_catalog, '"');
	string result;
	idx_t cursor = 0;
	for (idx_t index = 0; index + 1 < tokens.size(); index++) {
		auto identifier_end = TokenEnd(sql, tokens, index);
		auto identifier = sql.substr(tokens[index].start, identifier_end - tokens[index].start);
		if (tokens[index].type != SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER ||
		    (!StringUtil::CIEquals(identifier, client_catalog) && identifier != quoted_catalog)) {
			continue;
		}

		auto dot_end = TokenEnd(sql, tokens, index + 1);
		auto next_token = sql.substr(tokens[index + 1].start, dot_end - tokens[index + 1].start);
		if (tokens[index + 1].type != SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR || next_token != ".") {
			continue;
		}

		result.append(sql, cursor, tokens[index].start - cursor);
		cursor = dot_end;
		index++;
	}
	result.append(sql, cursor, sql.size() - cursor);
	return result;
}

void SetUnknownTransactionResponse(distributed::DistributedResponse &response, const string &message) {
	response.set_success(false);
	response.set_error_message(message);
	response.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_UNKNOWN);
}

} // namespace

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

DistributedFlightServerTestState &DistributedFlightServer::GetTestStateForTesting() {
	return test_state;
}

arrow::Status DistributedFlightServer::CheckRequestReplay(const distributed::DistributedRequest &request,
                                                          const ClientRegistration &registration,
                                                          ClientRequestTransport transport, const string &signature,
                                                          bool &replay) {
	replay = false;
	if (request.transaction_id() == INVALID_TRANSACTION_ID || request.request_sequence() == INVALID_REQUEST_SEQUENCE) {
		return arrow::Status::Invalid("Transaction identifier and request sequence must be specified");
	}
	if (request.transaction_mode() == distributed::TRANSACTION_MODE_AUTOCOMMIT) {
		if (registration.active_transaction_id != INVALID_TRANSACTION_ID) {
			return arrow::Status::Invalid("Autocommit request cannot run inside an explicit transaction");
		}
		if (request.request_sequence() != INITIAL_REQUEST_SEQUENCE) {
			return arrow::Status::Invalid("Autocommit request sequence must be one");
		}
		if (request.transaction_id() == registration.finished_transaction_id + 1) {
			return arrow::Status::OK();
		}
		if (request.transaction_id() != registration.finished_transaction_id) {
			return arrow::Status::Invalid("Autocommit transaction identifier is outside the replay window");
		}
	} else if (request.transaction_mode() != distributed::TRANSACTION_MODE_EXPLICIT) {
		return arrow::Status::Invalid("Transaction mode must be AUTOCOMMIT or EXPLICIT");
	} else if (registration.active_transaction_id != request.transaction_id()) {
		return arrow::Status::Invalid("Request does not belong to the active client transaction");
	} else if (request.request_sequence() > registration.last_request_sequence) {
		if (request.request_sequence() != registration.last_request_sequence + 1) {
			return arrow::Status::Invalid("Request sequence contains a gap");
		}
		return arrow::Status::OK();
	}
	if (request.request_sequence() < registration.last_request_sequence) {
		return arrow::Status::Invalid("Request sequence is older than the replay window");
	}
	if (registration.last_request_transport != transport || registration.last_request_signature != signature) {
		return arrow::Status::Invalid("Request sequence was reused for a different operation");
	}
	if (transport == ClientRequestTransport::DO_GET) {
		if (!registration.last_query_schema) {
			return arrow::Status::Invalid("Query result is unavailable for replay");
		}
	} else if (registration.last_action_response.empty()) {
		return arrow::Status::Invalid("Operation result is unavailable for replay");
	}
	replay = true;
	return arrow::Status::OK();
}

void DistributedFlightServer::ClearRequestReplay(ClientRegistration &registration) {
	registration.last_request_sequence = INVALID_REQUEST_SEQUENCE;
	registration.last_request_transport = ClientRequestTransport::NONE;
	registration.last_request_signature.clear();
	registration.last_action_response.clear();
	registration.last_query_schema.reset();
	registration.last_query_batches.clear();
}

void DistributedFlightServer::CacheActionResponse(const distributed::DistributedRequest &request,
                                                  ClientRegistration &registration, ClientRequestTransport transport,
                                                  const string &signature,
                                                  const distributed::DistributedResponse &response) {
	ClearRequestReplay(registration);
	registration.last_request_sequence = request.request_sequence();
	registration.last_request_transport = transport;
	registration.last_request_signature = signature;
	registration.last_action_response = response.SerializeAsString();
	if (request.transaction_mode() == distributed::TRANSACTION_MODE_AUTOCOMMIT) {
		registration.finished_transaction_id = request.transaction_id();
		registration.finished_transaction_status = distributed::TRANSACTION_STATUS_COMMITTED;
	}
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
	const auto expiration = GetSteadyNowMilliSecSinceEpoch() - test_state.GetClientLeaseTimeout().count();
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
	auto validation = ValidateRequest(req);
	if (!validation.ok()) {
		resp.set_success(false);
		resp.set_error_message(validation.message());
		return arrow::Status::OK();
	}
	const unique_lock<std::shared_mutex> lock(clients_mutex);
	PruneExpiredClients();
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

void DistributedFlightServer::ExecuteTransactionAction(const distributed::DistributedRequest &req,
                                                       ClientRegistration &registration,
                                                       distributed::DistributedResponse &resp) {
	auto action = req.transaction().action();
	if (action == distributed::TRANSACTION_ACTION_BEGIN) {
		if (registration.active_transaction_id != INVALID_TRANSACTION_ID) {
			if (registration.active_transaction_id != req.transaction_id()) {
				resp.set_success(false);
				resp.set_error_message("Another transaction is already active on this client connection");
				resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ACTIVE);
				return;
			}
			resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ACTIVE);
		} else if (req.transaction_id() <= registration.finished_transaction_id) {
			resp.set_success(false);
			resp.set_error_message("Transaction identifier has already been finalized");
			if (req.transaction_id() == registration.finished_transaction_id) {
				resp.mutable_transaction()->set_status(registration.finished_transaction_status);
			} else {
				resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_UNKNOWN);
			}
			return;
		} else if (req.transaction_id() != registration.finished_transaction_id + 1) {
			SetUnknownTransactionResponse(resp, "Transaction identifier is not the next expected value");
			return;
		} else {
			registration.connection->BeginTransaction();
			registration.active_transaction_id = req.transaction_id();
			ClearRequestReplay(registration);
			registration.last_request_sequence = req.request_sequence();
			resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ACTIVE);
		}
	} else {
		auto commit = action == distributed::TRANSACTION_ACTION_COMMIT;
		auto completed_status =
		    commit ? distributed::TRANSACTION_STATUS_COMMITTED : distributed::TRANSACTION_STATUS_ROLLED_BACK;
		auto action_name = commit ? "COMMIT" : "ROLLBACK";

		if (registration.active_transaction_id == req.transaction_id()) {
			if (req.request_sequence() <= registration.last_request_sequence) {
				SetUnknownTransactionResponse(
				    resp,
				    StringUtil::Format("%s request sequence is not newer than the previous operation", action_name));
				return;
			}
			if (commit) {
				registration.connection->Commit();
			} else {
				registration.connection->Rollback();
			}
			registration.active_transaction_id = INVALID_TRANSACTION_ID;
			registration.finished_transaction_id = req.transaction_id();
			registration.finished_transaction_status = completed_status;
			ClearRequestReplay(registration);
		} else if (registration.finished_transaction_id != req.transaction_id()) {
			SetUnknownTransactionResponse(
			    resp, StringUtil::Format("Remote Duckherder %s outcome is unknown: transaction state is unavailable",
			                             action_name));
			return;
		}
		resp.mutable_transaction()->set_status(registration.finished_transaction_status);
		if (registration.finished_transaction_status != completed_status) {
			resp.set_success(false);
			resp.set_error_message(StringUtil::Format("Remote Duckherder transaction was already %s",
			                                          commit ? "rolled back" : "committed"));
			return;
		}
	}
	resp.set_success(true);
}

arrow::Status DistributedFlightServer::HandleTransaction(const distributed::DistributedRequest &req,
                                                         ClientRegistration &registration,
                                                         distributed::DistributedResponse &resp) {
	test_state.RecordTransactionRequest();
	// Reject unspecified or unsupported lifecycle actions before reading transaction state.
	auto validation = ValidateRequest(req.transaction());
	if (!validation.ok()) {
		resp.set_success(false);
		resp.set_error_message(validation.message());
		return arrow::Status::OK();
	}
	// Both identifiers are required to distinguish a new lifecycle operation from its retries.
	if (req.transaction_id() == INVALID_TRANSACTION_ID || req.request_sequence() == INVALID_REQUEST_SEQUENCE) {
		resp.set_success(false);
		resp.set_error_message("Transaction identifier and request sequence must be specified");
		return arrow::Status::OK();
	}
	// BEGIN, COMMIT, and ROLLBACK belong to an explicit transaction; autocommit has no lifecycle RPCs.
	if (req.transaction_mode() != distributed::TRANSACTION_MODE_EXPLICIT) {
		resp.set_success(false);
		resp.set_error_message("Transaction lifecycle requests require EXPLICIT mode");
		return arrow::Status::OK();
	}

	// Tests use a delivered UNKNOWN response to exercise client-side outcome reconciliation.
	if (test_state.ShouldReturnUnknownTransactionResponse()) {
		SetUnknownTransactionResponse(resp, "Injected unknown transaction outcome");
		return arrow::Status::OK();
	}

	try {
		ExecuteTransactionAction(req, registration, resp);
	} catch (const std::exception &ex) {
		resp.set_success(false);
		resp.set_error_message(ex.what());
		resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_UNKNOWN);
		return arrow::Status::OK();
	}
	if (resp.success() && req.transaction().action() == distributed::TRANSACTION_ACTION_COMMIT &&
	    test_state.ShouldFailCommitResponse()) {
		return arrow::Status::IOError("Injected lost COMMIT response");
	}
	return arrow::Status::OK();
}

arrow::Status DistributedFlightServer::DoActionImpl(const arrow::flight::ServerCallContext &context,
                                                    const arrow::flight::Action &action,
                                                    std::unique_ptr<arrow::flight::ResultStream> *result) {
	distributed::DistributedRequest request;
	if (!request.ParseFromArray(action.body->data(), action.body->size())) {
		return arrow::Status::Invalid("Failed to parse DistributedRequest");
	}
	ARROW_RETURN_NOT_OK(ValidateRequest(request));

	distributed::DistributedResponse response;
	response.set_success(true);
	auto execute_idempotent_action = [&](ClientRegistration &registration, auto &&operation) -> arrow::Status {
		auto signature = request.SerializeAsString();
		bool replay = false;
		ARROW_RETURN_NOT_OK(
		    CheckRequestReplay(request, registration, ClientRequestTransport::ACTION, signature, replay));
		if (replay) {
			if (!response.ParseFromString(registration.last_action_response)) {
				return arrow::Status::Invalid("Failed to parse cached operation response");
			}
			return arrow::Status::OK();
		}
		ARROW_RETURN_NOT_OK(operation());
		CacheActionResponse(request, registration, ClientRequestTransport::ACTION, signature, response);
		return arrow::Status::OK();
	};

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
	case distributed::DistributedRequest::kTransaction:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(HandleTransaction(request, *registration, response));
		}
		break;
	case distributed::DistributedRequest::kExecuteStatement:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(execute_idempotent_action(*registration, [&] {
				return HandleExecuteStatement(request.execute_statement(), *registration, response);
			}));
		}
		break;
	case distributed::DistributedRequest::kTableExists:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(execute_idempotent_action(
			    *registration, [&] { return HandleTableExists(request.table_exists(), *registration, response); }));
		}
		break;
	case distributed::DistributedRequest::kLoadExtension:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_WRITE, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(execute_idempotent_action(
			    *registration, [&] { return HandleLoadExtension(request.load_extension(), *registration, response); }));
		}
		break;

	// ========== Stats & Monitoring Operations ==========
	case distributed::DistributedRequest::kGetQueryExecutionStats:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			const lock_guard<mutex> lock(registration->connection_mutex);
			ARROW_RETURN_NOT_OK(execute_idempotent_action(*registration, [&] {
				return HandleGetQueryExecutionStats(request.get_query_execution_stats(), response);
			}));
		}
		break;
	case distributed::DistributedRequest::kClientHeartbeat:
		if (AuthorizeClient(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
			response.mutable_client_heartbeat();
		}
		break;

	default:
		return arrow::Status::Invalid("Unknown request type");
	}
	if (request.request_case() == distributed::DistributedRequest::kExecuteStatement &&
	    test_state.ShouldFailExecuteStatementResponse()) {
		return arrow::Status::IOError("Injected lost execute-statement response");
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
	auto signature = request.SerializeAsString();
	bool replay = false;
	ARROW_RETURN_NOT_OK(CheckRequestReplay(request, *registration, ClientRequestTransport::DO_GET, signature, replay));
	if (!replay) {
		std::shared_ptr<arrow::Schema> schema;
		vector<std::shared_ptr<arrow::RecordBatch>> batches;
		ARROW_RETURN_NOT_OK(HandleScanTable(request.scan_table(), *registration, schema, batches));
		ClearRequestReplay(*registration);
		registration->last_request_sequence = request.request_sequence();
		registration->last_request_transport = ClientRequestTransport::DO_GET;
		registration->last_request_signature = signature;
		registration->last_query_schema = std::move(schema);
		registration->last_query_batches = std::move(batches);
		if (request.transaction_mode() == distributed::TRANSACTION_MODE_AUTOCOMMIT) {
			registration->finished_transaction_id = request.transaction_id();
			registration->finished_transaction_status = distributed::TRANSACTION_STATUS_COMMITTED;
		}
	}

	ARROW_ASSIGN_OR_RAISE(
	    auto reader, arrow::RecordBatchReader::Make(registration->last_query_batches, registration->last_query_schema));
	auto data_stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	if (test_state.ShouldFailScanResponse()) {
		return arrow::Status::IOError("Injected lost scan response");
	}

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
	if (descriptor.path.size() < 5) {
		return arrow::Status::Invalid("DoPut requires a registered client, table name, transaction identifier, "
		                              "request sequence, and transaction mode");
	}
	const auto &client_id = descriptor.path[0];
	const auto &table_name = descriptor.path[1];
	distributed::DistributedRequest request_identity;
	request_identity.set_transaction_id(std::stoull(descriptor.path[2]));
	request_identity.set_request_sequence(std::stoull(descriptor.path[3]));
	request_identity.set_transaction_mode(static_cast<distributed::TransactionMode>(std::stoi(descriptor.path[4])));
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

	// Read all record batches.
	ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto schema_text = schema->ToString();
	auto payload_hash = Hash(schema_text.c_str(), schema_text.size());
	while (true) {
		ARROW_ASSIGN_OR_RAISE(auto next, reader->Next());
		if (!next.data) {
			break;
		}
		auto batch_text = next.data->ToString();
		payload_hash = CombineHash(payload_hash, Hash(batch_text.c_str(), batch_text.size()));
		batches.emplace_back(std::move(next.data));
	}
	auto signature = StringUtil::Format("DoPut:%s:%llu:%llu:%llu", table_name, request_identity.transaction_id(),
	                                    request_identity.request_sequence(), payload_hash);
	bool replay = false;
	ARROW_RETURN_NOT_OK(
	    CheckRequestReplay(request_identity, *registration, ClientRequestTransport::DO_PUT, signature, replay));

	distributed::DistributedResponse resp;
	resp.set_success(true);
	if (replay && !resp.ParseFromString(registration->last_action_response)) {
		return arrow::Status::Invalid("Failed to parse cached insertion response");
	}
	if (!replay) {
		for (auto &batch : batches) {
			ARROW_RETURN_NOT_OK(HandleInsertData(table_name, batch, *registration, resp));
			if (!resp.success()) {
				break;
			}
		}
		CacheActionResponse(request_identity, *registration, ClientRequestTransport::DO_PUT, signature, resp);
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

arrow::Status DistributedFlightServer::HandleExecuteStatement(const distributed::ExecuteStatementRequest &req,
                                                              ClientRegistration &registration,
                                                              distributed::DistributedResponse &resp) {
	auto sql = StripClientCatalog(req.sql(), req.client_catalog());
	auto result = registration.connection->Query(sql);
	if (result->HasError()) {
		resp.set_success(false);
		resp.set_error_message(result->GetError());
		return arrow::Status::OK();
	}
	resp.set_success(true);
	resp.mutable_execute_statement();
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
                                                       std::shared_ptr<arrow::Schema> &schema,
                                                       vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
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

	return QueryResultToArrow(*result, schema, batches);
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

arrow::Status DistributedFlightServer::QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::Schema> &schema,
                                                          vector<std::shared_ptr<arrow::RecordBatch>> &batches,
                                                          idx_t *row_count) {
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, result.client_properties);
	ARROW_ASSIGN_OR_RAISE(schema, arrow::ImportSchema(&arrow_schema));

	// Collect all data chunks and convert to Arrow RecordBatches.
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
