#pragma once

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;
class MotherduckCatalog;

class MotherduckIndexCatalogEntry : public DuckIndexEntry {
public:
	// For local indexes (with storage)
	MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
	                            SchemaCatalogEntry &schema, CreateIndexInfo &info, TableCatalogEntry &table);
	
	// For remote indexes (without storage)
	MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
	                            SchemaCatalogEntry &schema, CreateIndexInfo &info);
	
	~MotherduckIndexCatalogEntry() override = default;

	bool IsRemoteIndex() const {
		return is_remote;
	}
	
	// Override for remote indexes that don't have storage
	string GetSchemaName() const override;
	string GetTableName() const override;
	void Rollback(CatalogEntry &prev_entry) override;
	void CommitDrop();  // Not virtual, but we define it to handle remote case

private:
	DatabaseInstance &db_instance;
	Catalog &motherduck_catalog_ref;
	bool is_remote;
	// For remote indexes, store schema and table names directly
	string remote_schema_name;
	string remote_table_name;
};

} // namespace duckdb
