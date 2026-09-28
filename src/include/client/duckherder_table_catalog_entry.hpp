#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
struct BoundCreateTableInfo;
class DatabaseInstance;
class DuckherderCatalog;
class DuckherderSchemaCatalogEntry;

class DuckherderTableCatalogEntry : public DuckTableEntry {
public:
	~DuckherderTableCatalogEntry() override = default;

	// Route table scans through the distributed client.
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;

private:
	friend class DuckherderSchemaCatalogEntry;

	DuckherderTableCatalogEntry(DuckherderCatalog &duckherder_catalog_p, DatabaseInstance &db_instance_p,
	                            DuckTableEntry *duck_table_entry_p,
	                            unique_ptr<BoundCreateTableInfo> create_table_info_p);

	DatabaseInstance &db_instance;
	unique_ptr<BoundCreateTableInfo> bound_create_table_info;
	DuckherderCatalog &duckherder_catalog;
};

} // namespace duckdb
