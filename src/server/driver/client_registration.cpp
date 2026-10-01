#include "server/driver/client_registration.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistration::ClientRegistration(shared_ptr<ObjectStorageDatabase> db, WorkerManager &worker_manager,
                                       distributed::ClientRole role_p, const distributed::StorageConfig &storage_config)
    : role(role_p), database_key(ObjectStorageDatabase::GetKey(storage_config)), database(db->GetSharedInstance()),
      last_seen(GetSteadyNowMilliSecSinceEpoch()), connection(db->Connect()) {
	// In-memory storage is private to this control-node instance. Local-storage readers can use worker snapshots.
	if (role != distributed::CLIENT_ROLE_READ_WRITE &&
	    storage_config.storage_case() == distributed::StorageConfig::kLocal) {
		distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection, storage_config);
	}
}

ClientRegistration::~ClientRegistration() = default;

} // namespace duckdb
