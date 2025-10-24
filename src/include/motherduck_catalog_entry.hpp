#pragma once

#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

class MotherduckCatalogEntry : public CatalogEntry {
public:
	MotherduckCatalogEntry(DatabaseInstance &db_instance_p, CatalogEntry *catalog_entry_p);
	~MotherduckCatalogEntry() override = default;

	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
	void UndoAlter(ClientContext &context, AlterInfo &info) override;
	void Rollback(CatalogEntry &prev_entry) override;
	void OnDrop() override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const;
	unique_ptr<CreateInfo> GetInfo() const;
	void SetAsRoot() override;
	string ToSQL() const;

	Catalog &ParentCatalog() override;
	const Catalog &ParentCatalog() const;
	SchemaCatalogEntry &ParentSchema() override;
	const SchemaCatalogEntry &ParentSchema() const;

	void Verify(Catalog &catalog) override;

private:
	DatabaseInstance &db_instance;
	CatalogEntry *catalog_entry;
};

} // namespace duckdb
