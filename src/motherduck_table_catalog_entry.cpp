#include "motherduck_table_catalog_entry.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

MotherduckTableCatalogEntry::MotherduckTableCatalogEntry(DatabaseInstance &db_instance_p,
                                                         TableCatalogEntry *table_catalog_entry_p,
                                                         unique_ptr<CreateTableInfo> create_table_info_p)
    : TableCatalogEntry(table_catalog_entry_p->catalog, table_catalog_entry_p->schema, *create_table_info_p),
      db_instance(db_instance_p), create_table_info(std::move(create_table_info_p)),
      table_catalog_entry(table_catalog_entry_p) {
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::AlterEntry");
	return table_catalog_entry->AlterEntry(context, info);
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::AlterEntry (CatalogTransaction)");
	return table_catalog_entry->AlterEntry(std::move(transaction), info);
}

void MotherduckTableCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::UndoAlter");
	table_catalog_entry->UndoAlter(context, info);
}

void MotherduckTableCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Rollback");
	table_catalog_entry->Rollback(prev_entry);
}

void MotherduckTableCatalogEntry::OnDrop() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::OnDrop");
	table_catalog_entry->OnDrop();
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Copy");
	return table_catalog_entry->Copy(context);
}

unique_ptr<CreateInfo> MotherduckTableCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetInfo");
	return table_catalog_entry->GetInfo();
}

void MotherduckTableCatalogEntry::SetAsRoot() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::SetAsRoot");
	table_catalog_entry->SetAsRoot();
}

string MotherduckTableCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ToSQL");
	return table_catalog_entry->ToSQL();
}

Catalog &MotherduckTableCatalogEntry::ParentCatalog() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentCatalog");
	return table_catalog_entry->ParentCatalog();
}

const Catalog &MotherduckTableCatalogEntry::ParentCatalog() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentCatalog (const)");
	return table_catalog_entry->ParentCatalog();
}

SchemaCatalogEntry &MotherduckTableCatalogEntry::ParentSchema() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentSchema");
	return table_catalog_entry->ParentSchema();
}

const SchemaCatalogEntry &MotherduckTableCatalogEntry::ParentSchema() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentSchema (const)");
	return table_catalog_entry->ParentSchema();
}

void MotherduckTableCatalogEntry::Verify(Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Verify");
	table_catalog_entry->Verify(catalog);
}

unique_ptr<BaseStatistics> MotherduckTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetStatistics");
	return table_catalog_entry->GetStatistics(context, std::move(column_id));
}

TableFunction MotherduckTableCatalogEntry::GetScanFunction(ClientContext &context,
                                                           unique_ptr<FunctionData> &bind_data) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetScanFunction");
	return table_catalog_entry->GetScanFunction(context, bind_data);
}

TableFunction MotherduckTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                           const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetScanFunction");
	return table_catalog_entry->GetScanFunction(context, bind_data, lookup_info);
}

bool MotherduckTableCatalogEntry::IsDuckTable() const {
	return table_catalog_entry->IsDuckTable();
}

TableStorageInfo MotherduckTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetStorageInfo");
	return table_catalog_entry->GetStorageInfo(context);
}
void MotherduckTableCatalogEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                                        LogicalUpdate &update, ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::BindUpdateConstraints");
	return table_catalog_entry->BindUpdateConstraints(binder, get, proj, update, context);
}

virtual_column_map_t MotherduckTableCatalogEntry::GetVirtualColumns() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetVirtualColumns");
	return table_catalog_entry->GetVirtualColumns();
}

vector<column_t> MotherduckTableCatalogEntry::GetRowIdColumns() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetRowIdColumns");
	return table_catalog_entry->GetRowIdColumns();
}

} // namespace duckdb
