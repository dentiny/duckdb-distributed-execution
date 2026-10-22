#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "utils/mutex.hpp"

namespace duckdb {

struct CreateIndexInfo;
class TableCatalogEntry;

// Metadata for the indexes of one schema that are physically stored on the control node.
class RemoteIndexRegistry {
public:
	void Add(TableCatalogEntry &table, const CreateIndexInfo &info);
	vector<IndexInfo> Get(const Identifier &table_name);
	void RemoveIndex(const Identifier &index_name);
	void RemoveTable(const Identifier &table_name);

private:
	struct RemoteIndexMetadata {
		Identifier name;
		IndexInfo info;
	};

	concurrency::mutex mu;
	// Keyed by table name.
	identifier_map_t<vector<RemoteIndexMetadata>> indexes DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb
