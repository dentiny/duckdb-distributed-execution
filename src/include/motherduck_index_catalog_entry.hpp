#pragma once

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;
class MotherduckCatalog;

// Motherduck LOCAL index catalog entry - wraps DuckIndexEntry for local tables
class MotherduckIndexCatalogEntry : public DuckIndexEntry {
public:
	MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
	                            SchemaCatalogEntry &schema, CreateIndexInfo &info, TableCatalogEntry &table);

	~MotherduckIndexCatalogEntry() override;

private:
	DatabaseInstance &db_instance;
	Catalog &motherduck_catalog_ref;
};

// Remote index catalog entry - represents an index on a remote table
// Inherits from DuckIndexEntry to satisfy transaction commit code,
// but provides dummy storage infrastructure that safely no-ops
class RemoteIndexCatalogEntry : public DuckIndexEntry {
public:
	RemoteIndexCatalogEntry(Catalog &motherduck_catalog_p, SchemaCatalogEntry &schema, CreateIndexInfo &info);

	~RemoteIndexCatalogEntry() override;

	// Override for remote indexes
	string GetSchemaName() const override;
	string GetTableName() const override;
	void Rollback(CatalogEntry &prev_entry) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	// Get the dummy data table info for remote indexes
	static shared_ptr<DataTableInfo> GetDummyDataTableInfo(Catalog &catalog);

private:
	Catalog &motherduck_catalog_ref;
	// For remote indexes, store schema and table names directly
	string remote_schema_name;
	string remote_table_name;
};

} // namespace duckdb
