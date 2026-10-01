#pragma once

#include "client.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "storage_config.pb.h"

namespace duckdb {

struct ClientRegistration;
class ObjectStorageDatabase;
class WorkerManager;

// One database served by the control node, with any number of read-only clients and at most one writable client. Like
// connections in one DuckDB process, all clients share one read-write instance, so readers see the writer's commits;
// read-only clients are kept from writing by their role. Not thread-safe; the server serializes admission.
class ServedDatabase {
public:
	// The Duckling catalog, whose instance is owned by the server.
	explicit ServedDatabase(shared_ptr<DuckDB> duckling);
	// An object storage database, opened for its first client.
	explicit ServedDatabase(distributed::StorageConfig config_p);

	// Admit a client with its own connection. Throws if the client would be the database's second writer.
	shared_ptr<ClientRegistration> AddClient(distributed::ClientRole role, WorkerManager &worker_manager);
	void RemoveClient(distributed::ClientRole role);
	bool HasClients() const;

private:
	const distributed::StorageConfig config;
	shared_ptr<DuckDB> duckling_instance;
	shared_ptr<ObjectStorageDatabase> object_storage_database;
	bool has_writer = false;
	idx_t reader_count = 0;
};

} // namespace duckdb
