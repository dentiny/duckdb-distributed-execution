#pragma once

#include "duckdb.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "storage_config.pb.h"

namespace duckdb {

// Catalog alias of the object storage database inside every instance returned by OpenObjectStorageDatabase.
inline constexpr const char *OBJECT_STORAGE_CATALOG = "object_db";

// Return true if the configuration selects an object storage database instead of the Duckling catalog.
bool HasObjectStorage(const distributed::StorageConfig &config);

// Map key used by control nodes and workers to reuse the DuckDB instance for one object storage database. The full
// serialized configuration is included, so the same database URI under a different backend or root gets a separate
// instance.
string GetStorageKey(const distributed::StorageConfig &config);

// Create a DuckDB instance dedicated to one validated storage configuration, with the database attached as
// OBJECT_STORAGE_CATALOG in the requested access mode. ObjFS settings are frozen per instance, so an instance
// must never be shared between configurations.
unique_ptr<DuckDB> OpenObjectStorageDatabase(const distributed::StorageConfig &config, AccessMode access_mode);

// Create a connection whose default catalog is OBJECT_STORAGE_CATALOG.
unique_ptr<Connection> ConnectObjectStorageDatabase(DuckDB &db);

} // namespace duckdb
