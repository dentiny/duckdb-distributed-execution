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
//
// A request holds `mutex` only to look up its registration, then runs under the registration's `connection_mutex`,
// so a slow request does not block other clients from registering. Requests must check `closed` once they hold
// `connection_mutex`. Lock order is `mutex` before `connection_mutex`.
class ClientRegistry {
public:
	static constexpr const char *NOT_REGISTERED_ERROR = "Duckherder client is not registered with the control node";

	explicit ClientRegistry(DistributedFlightServerTestState &test_state);

	// Register a new client after pruning expired ones.
	arrow::Status Register(const distributed::RegisterClientRequest &req, WorkerManager &worker_manager,
	                       distributed::DistributedResponse &resp) DUCKDB_EXCLUDES(mutex);
	// Unregister a client if it is still registered, after its in-flight request finishes.
	arrow::Status Unregister(const string &client_id, distributed::DistributedResponse &resp) DUCKDB_EXCLUDES(mutex);
	// Drop every client and database, after their in-flight requests finish.
	void Clear() DUCKDB_EXCLUDES(mutex);

	bool Lookup(const string &client_id, shared_ptr<ClientRegistration> &registration) DUCKDB_EXCLUDES(mutex);
	// Validate registration against the minimum role, renewing the lease on success.
	bool Authorize(const string &client_id, distributed::ClientRole required_role,
	               shared_ptr<ClientRegistration> &registration, distributed::DistributedResponse &resp)
	    DUCKDB_EXCLUDES(mutex);
	// Renew a client's lease after an authorized request.
	static void Touch(const shared_ptr<ClientRegistration> &registration);

private:
	using ClientMap = unordered_map<string, shared_ptr<ClientRegistration>>;

	// Close a registration after its in-flight request finishes.
	static void Close(ClientRegistration &registration) DUCKDB_EXCLUDES(registration.connection_mutex);
	// Close a registration unless it has a request in flight, which means it is still in use.
	static bool TryClose(ClientRegistration &registration) DUCKDB_EXCLUDES(registration.connection_mutex);
	// Remove expired clients, skipping those with a request in flight.
	void PruneExpired() DUCKDB_REQUIRES(mutex);
	// Detach a client and drop a non-default database after its final client leaves.
	void Remove(ClientMap::iterator entry) DUCKDB_REQUIRES(mutex);

	mutable concurrency::shared_mutex mutex;
	DistributedFlightServerTestState &test_state;
	// Keyed by client id.
	ClientMap clients DUCKDB_GUARDED_BY(mutex);
	// Control-node databases keyed by storage identity without credentials. The default in-memory database is
	// retained after its final client leaves.
	unordered_map<string, unique_ptr<ServedDatabase>> databases DUCKDB_GUARDED_BY(mutex);
};

} // namespace duckdb
