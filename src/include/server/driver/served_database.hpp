#pragma once

#include "client.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "storage_config.pb.h"

#include <arrow/result.h>

namespace duckdb {

struct ClientRegistration;
class ObjectStorageDatabase;
class WorkerManager;

// One database served by the control node, with any number of read-only clients and at most one writable client. Like
// connections in one DuckDB process, all clients share one read-write instance, so readers see the writer's commits;
// read-only clients are kept from writing by their role. Not thread-safe; the server serializes admission.
class ServedDatabase {
public:
	// The object storage database is opened for its first client.
	explicit ServedDatabase(distributed::StorageConfig config_p);

	// Admit a client with its own connection.
	arrow::Result<shared_ptr<ClientRegistration>> AddClient(distributed::ClientRole role,
	                                                        WorkerManager &worker_manager);
	void RemoveClient(distributed::ClientRole role);
	bool HasClients() const;
	bool IsDefault() const;

private:
	const distributed::StorageConfig config;
	unique_ptr<ObjectStorageDatabase> database;
	bool has_writer = false;
	idx_t reader_count = 0;
};

} // namespace duckdb
