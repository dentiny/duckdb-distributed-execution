#include "server/driver/client_registration.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistration::ClientRegistration(ObjectStorageDatabase &db, unique_ptr<Connection> connection_p,
                                       WorkerManager &worker_manager, distributed::ClientRole role_p,
                                       const distributed::StorageConfig &storage_config)
    : role(role_p), database_key(ObjectStorageDatabase::GetKey(storage_config)), database(db.GetSharedInstance()),
      last_seen(GetSteadyNowMilliSecSinceEpoch()), connection(std::move(connection_p)) {
	// In-memory storage is private to this control-node instance. Shared-storage readers can use worker snapshots.
	if (role != distributed::CLIENT_ROLE_READ_WRITE &&
	    storage_config.storage_case() != distributed::StorageConfig::kInMemory) {
		distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection, storage_config);
	}
}

ClientRegistration::~ClientRegistration() = default;

} // namespace duckdb
