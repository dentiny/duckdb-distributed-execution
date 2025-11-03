#pragma once

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;
class MotherduckCatalog;

// Motherduck index catalog entry, which represents an index on a remote table.
// It inherits from DuckIndexEntry to satisfy transaction commit code, but provides dummy storage infrastructure that
// safely no-ops.
class MotherduckIndexCatalogEntry : public DuckIndexEntry {
public:
	MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, SchemaCatalogEntry &schema, CreateIndexInfo &info);

	~MotherduckIndexCatalogEntry() override;

	string GetSchemaName() const override;
	string GetTableName() const override;
	void Rollback(CatalogEntry &prev_entry) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	// Get the dummy data table info for remote indexes.
	static shared_ptr<DataTableInfo> GetDummyDataTableInfo(Catalog &catalog);

private:
	Catalog &motherduck_catalog_ref;
	// For remote indexes, store schema and table names directly.
	string remote_schema_name;
	string remote_table_name;
};

} // namespace duckdb
