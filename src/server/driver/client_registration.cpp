#include "server/driver/client_registration.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistration::ClientRegistration(ObjectStorageDatabase &db, unique_ptr<Connection> connection_p,
                                       unique_ptr<Connection> executor_connection_p, WorkerManager &worker_manager,
                                       distributed::ClientRole role_p, const distributed::StorageConfig &storage_config)
    : role(role_p), database_key(ObjectStorageDatabase::GetKey(storage_config)), database(db.GetSharedInstance()),
      last_seen(GetSteadyNowMilliSecSinceEpoch()), connection(std::move(connection_p)),
      executor_connection(std::move(executor_connection_p)) {
	if (executor_connection != nullptr) {
		distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *executor_connection, storage_config);
		worker_fragments = make_shared_ptr<WorkerFragmentState>(*distributed_executor, *executor_connection);
		connection->context->registered_state->Insert(WorkerFragmentState::NAME, worker_fragments);
	}
}

ClientRegistration::~ClientRegistration() = default;

arrow::Status ClientRegistration::CheckRequestReplay(const distributed::DistributedRequest &request,
                                                     ClientRequestTransport transport, const string &signature,
                                                     bool &replay) const {
	replay = false;
	if (request.transaction_id() == INVALID_TRANSACTION_ID || request.request_sequence() == INVALID_REQUEST_SEQUENCE) {
		return arrow::Status::Invalid("Transaction identifier and request sequence must be specified");
	}
	if (request.transaction_mode() == distributed::TRANSACTION_MODE_AUTOCOMMIT) {
		if (active_transaction_id != INVALID_TRANSACTION_ID) {
			return arrow::Status::Invalid("Autocommit request cannot run inside an explicit transaction");
		}
		if (request.request_sequence() != INITIAL_REQUEST_SEQUENCE) {
			return arrow::Status::Invalid("Autocommit request sequence must be one");
		}
		if (request.transaction_id() == finished_transaction_id + 1) {
			return arrow::Status::OK();
		}
		if (request.transaction_id() != finished_transaction_id) {
			return arrow::Status::Invalid("Autocommit transaction identifier is outside the replay window");
		}
	} else if (request.transaction_mode() != distributed::TRANSACTION_MODE_EXPLICIT) {
		return arrow::Status::Invalid("Transaction mode must be AUTOCOMMIT or EXPLICIT");
	} else if (active_transaction_id != request.transaction_id()) {
		return arrow::Status::Invalid("Request does not belong to the active client transaction");
	} else if (request.request_sequence() > last_request_sequence) {
		if (request.request_sequence() != last_request_sequence + 1) {
			return arrow::Status::Invalid("Request sequence contains a gap");
		}
		return arrow::Status::OK();
	}
	if (request.request_sequence() < last_request_sequence) {
		return arrow::Status::Invalid("Request sequence is older than the replay window");
	}
	if (last_request_transport != transport || last_request_signature != signature) {
		return arrow::Status::Invalid("Request sequence was reused for a different operation");
	}
	if (transport == ClientRequestTransport::DO_GET) {
		if (!last_query_schema) {
			return arrow::Status::Invalid("Query result is unavailable for replay");
		}
	} else if (last_action_response.empty()) {
		return arrow::Status::Invalid("Operation result is unavailable for replay");
	}
	replay = true;
	return arrow::Status::OK();
}

void ClientRegistration::CacheActionResponse(const distributed::DistributedRequest &request,
                                             ClientRequestTransport transport, const string &signature,
                                             const distributed::DistributedResponse &response) {
	RecordCompletedRequest(request, transport, signature);
	last_action_response = response.SerializeAsString();
}

void ClientRegistration::CacheQueryResult(const distributed::DistributedRequest &request, const string &signature,
                                          std::shared_ptr<arrow::Schema> schema,
                                          vector<std::shared_ptr<arrow::RecordBatch>> batches) {
	RecordCompletedRequest(request, ClientRequestTransport::DO_GET, signature);
	last_query_schema = std::move(schema);
	last_query_batches = std::move(batches);
}

void ClientRegistration::ClearRequestReplay() {
	last_request_sequence = INVALID_REQUEST_SEQUENCE;
	last_request_transport = ClientRequestTransport::NONE;
	last_request_signature.clear();
	last_action_response.clear();
	last_query_schema.reset();
	last_query_batches.clear();
}

void ClientRegistration::RecordCompletedRequest(const distributed::DistributedRequest &request,
                                                ClientRequestTransport transport, const string &signature) {
	ClearRequestReplay();
	last_request_sequence = request.request_sequence();
	last_request_transport = transport;
	last_request_signature = signature;
	if (request.transaction_mode() == distributed::TRANSACTION_MODE_AUTOCOMMIT) {
		finished_transaction_id = request.transaction_id();
		finished_transaction_status = distributed::TRANSACTION_STATUS_COMMITTED;
	}
}

} // namespace duckdb
