#pragma once

#include "client.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "storage.pb.h"

namespace duckdb {

struct ClientRegistration;
class WorkerManager;

// One database served by the control node. It takes connections from any number of read-only clients and at most one
// writable client. Not thread-safe; the server serializes admission.
class ServedDatabase {
public:
	// The Duckling catalog, whose instance serves both roles.
	explicit ServedDatabase(shared_ptr<DuckDB> duckling);
	// An object storage database. Readers share one read-only instance and the writer gets a read-write instance; each
	// is opened for its first client and released when its last client leaves.
	explicit ServedDatabase(distributed::StorageConfig config_p);

	// Admit a client with its own connection. Throws if the client would be the database's second writer.
	shared_ptr<ClientRegistration> AddClient(const string &client_id, distributed::ClientRole role,
	                                         WorkerManager &worker_manager);
	void RemoveClient(const string &client_id);
	bool HasClients() const;

private:
	const distributed::StorageConfig config;
	shared_ptr<DuckDB> reader_instance;
	shared_ptr<DuckDB> writer_instance;
	string writer_client_id;
	unordered_set<string> reader_client_ids;
};

} // namespace duckdb
