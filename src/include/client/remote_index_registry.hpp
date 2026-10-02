#pragma once

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
	vector<IndexInfo> Get(const string &table_name);
	void RemoveIndex(const string &index_name);
	void RemoveTable(const string &table_name);

private:
	struct RemoteIndexMetadata {
		string name;
		IndexInfo info;
	};

	concurrency::mutex mu;
	// Keyed by table name.
	unordered_map<string, vector<RemoteIndexMetadata>> indexes DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb
