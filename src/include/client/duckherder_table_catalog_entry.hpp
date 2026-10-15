#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
struct BoundCreateTableInfo;
class DuckherderCatalog;
class DuckherderSchemaCatalogEntry;

class DuckherderTableCatalogEntry : public DuckTableEntry {
public:
	~DuckherderTableCatalogEntry() override = default;

	// Route table scans through the distributed client.
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	// Returns local table storage metadata together with remote index metadata used by the binder.
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	friend class DuckherderSchemaCatalogEntry;

	DuckherderTableCatalogEntry(DuckherderCatalog &duckherder_catalog, DuckTableEntry *duck_table_entry_p,
	                            unique_ptr<BoundCreateTableInfo> create_table_info_p);

	unique_ptr<BoundCreateTableInfo> bound_create_table_info;
};

} // namespace duckdb
