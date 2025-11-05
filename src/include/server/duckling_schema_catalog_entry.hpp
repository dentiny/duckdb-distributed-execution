#pragma once

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;
class DucklingCatalog;

class DucklingSchemaCatalogEntry : public DuckSchemaEntry {
public:
	DucklingSchemaCatalogEntry(Catalog &duckling_catalog_p, DatabaseInstance &db_instance_p,
	                           SchemaCatalogEntry *schema_catalog_entry_p,
	                           unique_ptr<CreateSchemaInfo> create_schema_info_p);
	~DucklingSchemaCatalogEntry() override = default;

	//===--------------------------------------------------------------------===//
	// CatalogEntry-specific functions (delegate to underlying schema)
	//===--------------------------------------------------------------------===//
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
	void UndoAlter(ClientContext &context, AlterInfo &info) override;
	void Rollback(CatalogEntry &prev_entry) override;
	void OnDrop() override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CreateInfo> GetInfo() const override;
	void SetAsRoot() override;
	string ToSQL() const override;

	Catalog &ParentCatalog() override;
	const Catalog &ParentCatalog() const override;
	SchemaCatalogEntry &ParentSchema() override;
	const SchemaCatalogEntry &ParentSchema() const override;

	void Verify(Catalog &catalog) override;

private:
	DatabaseInstance &db_instance;
	unique_ptr<CreateSchemaInfo> create_schema_info;
	SchemaCatalogEntry *schema_catalog_entry;
	Catalog &duckling_catalog_ref;
};

} // namespace duckdb

