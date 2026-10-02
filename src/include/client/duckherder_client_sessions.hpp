#pragma once

#include "client.pb.h"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/typedefs.hpp"
#include "storage_config.pb.h"
#include "utils/mutex.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class DistributedClient;
class DuckherderConnectionState;

// Remote sessions of one Duckherder attachment, one per DuckDB connection. Each session is owned by its
// connection's registered state; this tracks them so detaching the attachment closes every session.
class DuckherderClientSessions {
public:
	DuckherderClientSessions(string server_url, distributed::ClientRole role, DatabaseInstance &db_instance,
	                         distributed::StorageConfig storage_config, connection_t attach_connection_id);
	~DuckherderClientSessions();

	// Get the remote session owned by this DuckDB connection, creating it on first use.
	DistributedClient &GetClient(ClientContext &context);
	// Close every session; later GetClient calls fail.
	void Close();
	// Drop the session registered in this connection.
	void RemoveState(ClientContext &context);

private:
	void EnsureWriteOwner(ClientContext &context) DUCKDB_REQUIRES(mu);
	shared_ptr<DuckherderConnectionState> GetOrCreateState(ClientContext &context) DUCKDB_REQUIRES(mu);
	void PruneExpiredStates() DUCKDB_REQUIRES(mu);

	const string server_url;
	const distributed::ClientRole role;
	DatabaseInstance &db_instance;
	// Every remote session of this attachment opens the same database on the control node.
	const distributed::StorageConfig storage_config;
	const string state_key;

	concurrency::mutex mu;
	connection_t attach_connection_id DUCKDB_GUARDED_BY(mu);
	bool detached DUCKDB_GUARDED_BY(mu) = false;
	unique_ptr<DistributedClient> attach_client DUCKDB_GUARDED_BY(mu);
	unordered_map<connection_t, weak_ptr<DuckherderConnectionState>> states DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb
