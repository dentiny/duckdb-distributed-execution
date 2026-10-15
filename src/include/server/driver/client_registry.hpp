#pragma once

#include "distributed.pb.h"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "server/driver/client_registration.hpp"
#include "server/driver/distributed_flight_server_test_state.hpp"
#include "server/driver/served_database.hpp"
#include "utils/mutex.hpp"

#include <arrow/status.h>

namespace duckdb {

class WorkerManager;

// Registered clients and the control-node databases they are attached to, with lease-based expiration.
class ClientRegistry {
public:
	static constexpr const char *NOT_REGISTERED_ERROR = "Duckherder client is not registered with the control node";

	explicit ClientRegistry(DistributedFlightServerTestState &test_state);

	// Register a new client after pruning expired ones.
	arrow::Status Register(const distributed::RegisterClientRequest &req, WorkerManager &worker_manager,
	                       distributed::DistributedResponse &resp) DUCKDB_EXCLUDES(mutex);
	// Unregister a client if it is still registered.
	arrow::Status Unregister(const string &client_id, distributed::DistributedResponse &resp) DUCKDB_EXCLUDES(mutex);
	// Drop every client and database.
	void Clear() DUCKDB_EXCLUDES(mutex);

	bool Lookup(const string &client_id, shared_ptr<ClientRegistration> &registration) DUCKDB_EXCLUDES(mutex);
	// Validate registration against the minimum role, renewing the lease on success.
	bool Authorize(const string &client_id, distributed::ClientRole required_role,
	               shared_ptr<ClientRegistration> &registration, distributed::DistributedResponse &resp)
	    DUCKDB_EXCLUDES(mutex);
	// Renew a client's lease after an authorized request.
	static void Touch(const shared_ptr<ClientRegistration> &registration);

private:
	// Move expired clients into `expired` and remove databases no client references anymore.
	void PruneExpired(vector<shared_ptr<ClientRegistration>> &expired) DUCKDB_REQUIRES(mutex);

	mutable concurrency::shared_mutex mutex;
	DistributedFlightServerTestState &test_state;
	// Keyed by client id.
	unordered_map<string, shared_ptr<ClientRegistration>> clients DUCKDB_GUARDED_BY(mutex);
	// Control-node databases keyed by storage identity without credentials, owned by their client registrations.
	unordered_map<string, weak_ptr<ServedDatabase>> databases DUCKDB_GUARDED_BY(mutex);
	// The default in-memory database is retained after its final client leaves.
	shared_ptr<ServedDatabase> default_database DUCKDB_GUARDED_BY(mutex);
};

} // namespace duckdb
