#include "duckling_table_catalog_entry.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckling_catalog.hpp"

namespace duckdb {

DucklingTableCatalogEntry::DucklingTableCatalogEntry(Catalog &duckling_catalog_p, DatabaseInstance &db_instance_p,
                                                     DuckTableEntry *duck_table_entry_p,
                                                     unique_ptr<BoundCreateTableInfo> bound_create_table_info_p)
    : DuckTableEntry(duckling_catalog_p, duck_table_entry_p->schema, *bound_create_table_info_p,
                     duck_table_entry_p->GetStorage().shared_from_this()),
      db_instance(db_instance_p), bound_create_table_info(std::move(bound_create_table_info_p)),
      duckling_catalog_ref(duckling_catalog_p) {
}

// Simple pass-through implementations - no-op wrapper for now
unique_ptr<CatalogEntry> DucklingTableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	return DuckTableEntry::AlterEntry(context, info);
}

unique_ptr<CatalogEntry> DucklingTableCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	return DuckTableEntry::AlterEntry(std::move(transaction), info);
}

void DucklingTableCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DuckTableEntry::UndoAlter(context, info);
}

void DucklingTableCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DuckTableEntry::Rollback(prev_entry);
}

void DucklingTableCatalogEntry::OnDrop() {
	DuckTableEntry::OnDrop();
}

unique_ptr<CatalogEntry> DucklingTableCatalogEntry::Copy(ClientContext &context) const {
	return DuckTableEntry::Copy(context);
}

unique_ptr<CreateInfo> DucklingTableCatalogEntry::GetInfo() const {
	return DuckTableEntry::GetInfo();
}

void DucklingTableCatalogEntry::SetAsRoot() {
	DuckTableEntry::SetAsRoot();
}

string DucklingTableCatalogEntry::ToSQL() const {
	return DuckTableEntry::ToSQL();
}

Catalog &DucklingTableCatalogEntry::ParentCatalog() {
	return DuckTableEntry::ParentCatalog();
}

const Catalog &DucklingTableCatalogEntry::ParentCatalog() const {
	return DuckTableEntry::ParentCatalog();
}

SchemaCatalogEntry &DucklingTableCatalogEntry::ParentSchema() {
	return DuckTableEntry::ParentSchema();
}

const SchemaCatalogEntry &DucklingTableCatalogEntry::ParentSchema() const {
	return DuckTableEntry::ParentSchema();
}

void DucklingTableCatalogEntry::Verify(Catalog &catalog) {
	DuckTableEntry::Verify(catalog);
}

bool DucklingTableCatalogEntry::IsDuckTable() const {
	return DuckTableEntry::IsDuckTable();
}

unique_ptr<BaseStatistics> DucklingTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return DuckTableEntry::GetStatistics(context, column_id);
}

TableFunction DucklingTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	return DuckTableEntry::GetScanFunction(context, bind_data);
}

TableFunction DucklingTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                         const EntryLookupInfo &lookup_info) {
	// DuckTableEntry doesn't have this overload, just call the base version
	return DuckTableEntry::GetScanFunction(context, bind_data);
}

TableStorageInfo DucklingTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	return DuckTableEntry::GetStorageInfo(context);
}

void DucklingTableCatalogEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                                      LogicalUpdate &update, ClientContext &context) {
	DuckTableEntry::BindUpdateConstraints(binder, get, proj, update, context);
}

virtual_column_map_t DucklingTableCatalogEntry::GetVirtualColumns() const {
	return DuckTableEntry::GetVirtualColumns();
}

vector<column_t> DucklingTableCatalogEntry::GetRowIdColumns() const {
	return DuckTableEntry::GetRowIdColumns();
}

} // namespace duckdb
