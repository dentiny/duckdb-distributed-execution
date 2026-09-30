#include "server/driver/client_registration.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistration::ClientRegistration(shared_ptr<DuckDB> db, WorkerManager &worker_manager,
                                       distributed::ClientRole role_p, distributed::StorageConfig storage_config_p)
    : role(role_p), storage_config(std::move(storage_config_p)), database(std::move(db)),
      last_seen(GetSteadyNowMilliSecSinceEpoch()) {
	if (!HasObjectStorage(storage_config)) {
		connection = make_uniq<Connection>(*database);
		auto use_result = connection->Query("USE duckling;");
		if (use_result->HasError()) {
			throw InternalException(
			    StringUtil::Format("Failed to USE duckling for client connection: %s", use_result->GetError()));
		}
		distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection, storage_config);
		return;
	}

	connection = ConnectObjectStorageDatabase(*database);
	// Workers read the snapshot they attached, so the writer's own reads must see its writes on the control node.
	if (role != distributed::CLIENT_ROLE_READ_WRITE) {
		distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection, storage_config);
	}
}

ClientRegistration::~ClientRegistration() = default;

} // namespace duckdb
