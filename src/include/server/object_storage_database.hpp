#pragma once

#include "duckdb.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "storage_config.pb.h"

#include <arrow/result.h>

namespace duckdb {

// One DuckDB instance configured for one object storage database. Control nodes and workers use this common wrapper
// with different access modes.
class ObjectStorageDatabase {
public:
	static arrow::Result<unique_ptr<ObjectStorageDatabase>> Create(const distributed::StorageConfig &config,
	                                                               AccessMode access_mode);

	// Create an independent session whose default catalog is this object storage database.
	arrow::Result<unique_ptr<Connection>> Connect() const;

	DuckDB &GetInstance() const;
	const shared_ptr<DuckDB> &GetSharedInstance() const;

	// Resolve an empty client configuration to the server's default in-memory database.
	static distributed::StorageConfig ResolveConfig(const distributed::StorageConfig &config);
	// Return whether the URI is reserved for the server-owned default in-memory database.
	static bool IsDefaultURI(const string &database_uri);
	// Stable identity used to reuse one instance for a database and storage location. Credentials are excluded so
	// different credentials cannot bypass single-writer admission for the same database.
	static string GetKey(const distributed::StorageConfig &config);

private:
	explicit ObjectStorageDatabase(shared_ptr<DuckDB> instance_p);

	shared_ptr<DuckDB> instance;
};

} // namespace duckdb
