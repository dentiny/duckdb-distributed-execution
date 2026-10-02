#pragma once

#include "distributed.pb.h"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
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
	explicit ClientRegistry(DistributedFlightServerTestState &test_state);

	// Held shared for the whole duration of a client request, so registrations cannot be removed while in use, and
	// exclusively while registering or removing clients.
	mutable concurrency::shared_mutex mutex;

	// Register a new client after pruning expired ones, taking `mutex` exclusively.
	arrow::Status Register(const distributed::RegisterClientRequest &req, WorkerManager &worker_manager,
	                       distributed::DistributedResponse &resp);
	// Unregister a client if it is still registered, taking `mutex` exclusively.
	arrow::Status Unregister(const string &client_id, distributed::DistributedResponse &resp);
	// Drop every client and database.
	void Clear() DUCKDB_REQUIRES(mutex);

	bool Lookup(const string &client_id, shared_ptr<ClientRegistration> &registration) DUCKDB_REQUIRES_SHARED(mutex);
	// Validate registration against the minimum role, renewing the lease on success.
	bool Authorize(const string &client_id, distributed::ClientRole required_role,
	               shared_ptr<ClientRegistration> &registration, distributed::DistributedResponse &resp)
	    DUCKDB_REQUIRES_SHARED(mutex);
	// Renew a client's lease after an authorized request.
	static void Touch(const shared_ptr<ClientRegistration> &registration);

private:
	using ClientMap = unordered_map<string, shared_ptr<ClientRegistration>>;

	void PruneExpired() DUCKDB_REQUIRES(mutex);
	// Detach a client and drop a non-default database after its final client leaves.
	void Remove(ClientMap::iterator entry) DUCKDB_REQUIRES(mutex);

	DistributedFlightServerTestState &test_state;
	// Keyed by client id.
	ClientMap clients DUCKDB_GUARDED_BY(mutex);
	// Control-node databases keyed by storage identity without credentials. The default in-memory database is
	// retained after its final client leaves.
	unordered_map<string, unique_ptr<ServedDatabase>> databases DUCKDB_GUARDED_BY(mutex);
};

} // namespace duckdb
