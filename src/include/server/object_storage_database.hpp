#pragma once

#include "duckdb.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "storage_config.pb.h"

namespace duckdb {

// One DuckDB instance configured for one object storage database. Control nodes and workers use this common wrapper
// with different access modes.
class ObjectStorageDatabase {
public:
	ObjectStorageDatabase(const distributed::StorageConfig &config, AccessMode access_mode);

	// Create an independent session whose default catalog is this object storage database.
	unique_ptr<Connection> Connect() const;

	DuckDB &GetInstance() const;
	const shared_ptr<DuckDB> &GetSharedInstance() const;

	// Return true if the configuration selects object storage instead of the Duckling catalog.
	// TODO(hjiang): remove duckling catalog support.
	static bool IsConfigured(const distributed::StorageConfig &config);
	// Map key used by control nodes and workers to reuse the instance for one configuration. The full serialized
	// configuration distinguishes the same database URI under different storage backends or roots.
	static string GetKey(const distributed::StorageConfig &config);

private:
	shared_ptr<DuckDB> instance;
};

} // namespace duckdb
