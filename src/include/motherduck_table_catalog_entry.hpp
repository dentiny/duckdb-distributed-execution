#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
class CreateTableInfo;
class DatabaseInstance;
class TableFunction;
class TableStorageInfo;

class MotherduckTableCatalogEntry : public TableCatalogEntry {
public:
	MotherduckTableCatalogEntry(DatabaseInstance &db_instance_p, TableCatalogEntry *table_catalog_entry_p,
	                            unique_ptr<CreateTableInfo> create_table_info_p);
	~MotherduckTableCatalogEntry() override = default;

	// CatalogEntry-specific functions.
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
	bool IsDuckTable() const override;

	// TableCatalogEntry-special functions.
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

private:
	DatabaseInstance &db_instance;
	unique_ptr<CreateTableInfo> create_table_info;
	TableCatalogEntry *table_catalog_entry;
};

} // namespace duckdb
