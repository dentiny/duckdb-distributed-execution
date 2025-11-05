#pragma once

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

// Duckling index catalog entry - simple no-op wrapper for now
class DucklingIndexCatalogEntry : public DuckIndexEntry {
public:
	DucklingIndexCatalogEntry(Catalog &duckling_catalog_p, SchemaCatalogEntry &schema, CreateIndexInfo &info);
	~DucklingIndexCatalogEntry() override;

	string GetSchemaName() const override;
	string GetTableName() const override;
	void Rollback(CatalogEntry &prev_entry) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	// Get the dummy data table info for indexes
	static shared_ptr<DataTableInfo> GetDummyDataTableInfo(Catalog &catalog);

private:
	Catalog &duckling_catalog_ref;
	string schema_name;
	string table_name;
};

} // namespace duckdb

