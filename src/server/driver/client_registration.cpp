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

} // namespace duckdb
