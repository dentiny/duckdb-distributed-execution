#include "core_functions_extension.hpp"
#include "server/driver/distributed_flight_server.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/logging/logger.hpp"
#include "server/startup_sql.hpp"
#include "server/validation.hpp"

#include <iostream>

namespace duckdb {

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
		ARROW_RETURN_NOT_OK(worker_manager->StartLocalWorkers(num_workers));
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Started %llu workers", num_workers));
	}

	// Start the server.
	return Start();
}

void DistributedFlightServer::Shutdown() {
	auto status = FlightServerBase::Shutdown();
	// Ignore shutdown errors in production
}

void DistributedFlightServer::Reset() {
	const concurrency::unique_lock<concurrency::shared_mutex> lock(clients.mutex);
	clients.Clear();
	Initialize();
}

void DistributedFlightServer::Initialize() {
	// Release objects that reference the previous database in dependency order.
	request_handler.reset();
	worker_manager.reset();
	db.reset();

	query_history.Clear();

	db = make_shared_ptr<DuckDB>(nullptr, nullptr);
	// Loadable extensions use DuckDB's dummy loader, so initialize core functions explicitly.
	db->LoadStaticExtension<CoreFunctionsExtension>();
	auto startup_status = RunStartupSQL(*db);
	if (!startup_status.ok()) {
		throw IOException(startup_status.ToString());
	}

	// Initialize the worker manager. Each client registration owns its connection-bound executor.
	worker_manager = make_uniq<WorkerManager>(*db);
	request_handler = make_uniq<ClientRequestHandler>(*db->instance, query_history);
}

string DistributedFlightServer::GetLocation() const {
	return StringUtil::Format("grpc://%s:%d", host, port);
}

arrow::Status DistributedFlightServer::RegisterWorker(const string &worker_id, const string &location) {
	if (!worker_manager) {
		return arrow::Status::Invalid("WorkerManager not initialized");
	}
	return worker_manager->RegisterWorker(worker_id, location);
}

arrow::Status DistributedFlightServer::RegisterOrReplaceDriver(const string &driver_id, const string &location) {
	if (!worker_manager) {
		return arrow::Status::Invalid("WorkerManager not initialized");
	}
	return worker_manager->RegisterOrReplaceDriver(driver_id, location);
}

idx_t DistributedFlightServer::GetWorkerCount() const {
	if (!worker_manager) {
		return 0;
	}
	return worker_manager->GetWorkerCount();
}

arrow::Status DistributedFlightServer::StartLocalWorkers(idx_t num_workers) {
	if (!worker_manager) {
		return arrow::Status::Invalid("WorkerManager not initialized");
	}
	return worker_manager->StartLocalWorkers(num_workers);
}

vector<QueryExecutionInfo> DistributedFlightServer::GetQueryExecutions() const {
	return query_history.Get();
}

DistributedFlightServerTestState &DistributedFlightServer::GetTestStateForTesting() {
	return test_state;
}

arrow::Status DistributedFlightServer::HandleClientAction(const distributed::DistributedRequest &request,
                                                          distributed::ClientRole required_role,
                                                          distributed::DistributedResponse &response) {
	shared_ptr<ClientRegistration> registration;
	if (!clients.Authorize(request.client_id(), required_role, registration, response)) {
		return arrow::Status::OK();
	}
	const concurrency::lock_guard<concurrency::mutex> lock(registration->connection_mutex);
	if (request.request_case() == distributed::DistributedRequest::kTransaction) {
		return transaction_handler.Handle(request, *registration, response);
	}
	return request_handler->HandleAction(request, *registration, response);
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

	if (request.request_case() == distributed::DistributedRequest::kRegisterClient) {
		ARROW_RETURN_NOT_OK(clients.Register(request.register_client(), *worker_manager, response));
	} else if (request.request_case() == distributed::DistributedRequest::kUnregisterClient) {
		ARROW_RETURN_NOT_OK(clients.Unregister(request.client_id(), response));
	} else {
		const concurrency::shared_lock<concurrency::shared_mutex> client_lock(clients.mutex);
		switch (request.request_case()) {
		case distributed::DistributedRequest::kExecuteStatement:
		case distributed::DistributedRequest::kLoadExtension:
			ARROW_RETURN_NOT_OK(HandleClientAction(request, distributed::CLIENT_ROLE_READ_WRITE, response));
			break;
		case distributed::DistributedRequest::kTransaction:
		case distributed::DistributedRequest::kTableExists:
		case distributed::DistributedRequest::kGetQueryExecutionStats:
			ARROW_RETURN_NOT_OK(HandleClientAction(request, distributed::CLIENT_ROLE_READ_ONLY, response));
			break;
		case distributed::DistributedRequest::kClientHeartbeat: {
			shared_ptr<ClientRegistration> registration;
			if (clients.Authorize(request.client_id(), distributed::CLIENT_ROLE_READ_ONLY, registration, response)) {
				response.mutable_client_heartbeat();
			}
			break;
		}
		default:
			return arrow::Status::Invalid("Unknown request type");
		}
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
	const concurrency::shared_lock<concurrency::shared_mutex> client_lock(clients.mutex);
	shared_ptr<ClientRegistration> registration;
	if (!clients.Lookup(request.client_id(), registration)) {
		return arrow::Status::Invalid("Duckherder client is not registered with the control node");
	}
	ClientRegistry::Touch(registration);

	const concurrency::lock_guard<concurrency::mutex> connection_lock(registration->connection_mutex);
	ARROW_RETURN_NOT_OK(request_handler->HandleScan(request, *registration));

	ARROW_ASSIGN_OR_RAISE(
	    auto reader, arrow::RecordBatchReader::Make(registration->last_query_batches, registration->last_query_schema));
	auto data_stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	if (test_state.ShouldFailScanResponse()) {
		return arrow::Status::IOError("Injected lost scan response");
	}

	ClientRegistry::Touch(registration);
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
	uint64_t transaction_id;
	uint64_t request_sequence;
	int32_t transaction_mode;
	if (!TryCast::Operation<string_t, uint64_t>(string_t(descriptor.path[2]), transaction_id) ||
	    !TryCast::Operation<string_t, uint64_t>(string_t(descriptor.path[3]), request_sequence) ||
	    !TryCast::Operation<string_t, int32_t>(string_t(descriptor.path[4]), transaction_mode)) {
		return arrow::Status::Invalid("DoPut transaction metadata must contain valid integers");
	}
	if (transaction_mode != distributed::TRANSACTION_MODE_AUTOCOMMIT &&
	    transaction_mode != distributed::TRANSACTION_MODE_EXPLICIT) {
		return arrow::Status::Invalid("DoPut transaction mode must be AUTOCOMMIT or EXPLICIT");
	}
	distributed::DistributedRequest request_identity;
	request_identity.set_transaction_id(transaction_id);
	request_identity.set_request_sequence(request_sequence);
	request_identity.set_transaction_mode(static_cast<distributed::TransactionMode>(transaction_mode));
	const concurrency::shared_lock<concurrency::shared_mutex> client_lock(clients.mutex);
	shared_ptr<ClientRegistration> registration;
	if (!clients.Lookup(client_id, registration)) {
		return arrow::Status::Invalid("Duckherder client is not registered with the control node");
	}
	if (registration->role != distributed::CLIENT_ROLE_READ_WRITE) {
		return arrow::Status::Invalid("Duckherder client is read-only");
	}
	ClientRegistry::Touch(registration);

	const concurrency::lock_guard<concurrency::mutex> connection_lock(registration->connection_mutex);

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

	distributed::DistributedResponse resp;
	ARROW_RETURN_NOT_OK(
	    request_handler->HandleInsert(request_identity, table_name, batches, signature, *registration, resp));

	// Write response metadata.
	std::string resp_data = resp.SerializeAsString();
	auto buffer = arrow::Buffer::FromString(resp_data);
	ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

	ClientRegistry::Touch(registration);
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

} // namespace duckdb
