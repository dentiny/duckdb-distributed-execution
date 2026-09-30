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
// writable client. As in DuckDB, where one process opens a database file once and all its connections share it, every
// client gets its own connection to one shared instance, so readers see what the writer has committed. Read-only
// clients are kept from writing by their role, not by the instance. Not thread-safe; the server serializes admission.
class ServedDatabase {
public:
	// The Duckling catalog, whose instance is owned by the server.
	explicit ServedDatabase(shared_ptr<DuckDB> duckling);
	// An object storage database, opened read-only until its first writer joins and read-write from then on, so that
	// a database with only readers never fences a writer in another process.
	explicit ServedDatabase(distributed::StorageConfig config_p);

	// Admit a client with its own connection. Throws if the client would be the database's second writer.
	shared_ptr<ClientRegistration> AddClient(const string &client_id, distributed::ClientRole role,
	                                         WorkerManager &worker_manager);
	void RemoveClient(const string &client_id);
	bool HasClients() const;

private:
	const distributed::StorageConfig config;
	// Clients admitted before the database became writable keep the read-only instance they connected to.
	shared_ptr<DuckDB> instance;
	bool instance_writable = false;
	string writer_client_id;
	unordered_set<string> reader_client_ids;
};

} // namespace duckdb
