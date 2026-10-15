#pragma once

#include "client.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "storage_config.pb.h"
#include "utils/mutex.hpp"

#include <arrow/result.h>

namespace duckdb {

struct ClientRegistration;
class ObjectStorageDatabase;
class WorkerManager;

// One database served by the control node, with any number of read-only clients and at most one writable client. Like
// connections in one DuckDB process, all clients share one read-write instance, so readers see the writer's commits;
// read-only clients are kept from writing by their role. Each client registration owns a reference, so the database
// and a client's admission outlive any request still using that client.
class ServedDatabase : public enable_shared_from_this<ServedDatabase> {
public:
	// The object storage database is opened for its first client.
	explicit ServedDatabase(distributed::StorageConfig config_p);

	// Admit a client with its own connection.
	arrow::Result<shared_ptr<ClientRegistration>> AddClient(distributed::ClientRole role, WorkerManager &worker_manager)
	    DUCKDB_EXCLUDES(mutex);
	// Release a client's admission; called when its registration is destroyed.
	void RemoveClient(distributed::ClientRole role) DUCKDB_EXCLUDES(mutex);
	bool IsDefault() const;

private:
	const distributed::StorageConfig config;
	concurrency::mutex mutex;
	unique_ptr<ObjectStorageDatabase> database DUCKDB_GUARDED_BY(mutex);
	bool has_writer DUCKDB_GUARDED_BY(mutex) = false;
};

} // namespace duckdb
