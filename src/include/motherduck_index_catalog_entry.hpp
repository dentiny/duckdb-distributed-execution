#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;
class MotherduckCatalog;

class MotherduckIndexCatalogEntry : public IndexCatalogEntry {
public:
	MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
	                            IndexCatalogEntry *duck_index_entry_p, CreateIndexInfo &info_p);
	~MotherduckIndexCatalogEntry() override = default;

	// CatalogEntry-specific functions.
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
	void UndoAlter(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CreateInfo> GetInfo() const override;
	string ToSQL() const override;

	// IndexCatalogEntry-specific functions.
	string GetSchemaName() const override;
	string GetTableName() const override;

private:
	DatabaseInstance &db_instance;
	IndexCatalogEntry *duck_index_entry;
	Catalog &motherduck_catalog_ref;
};

} // namespace duckdb
