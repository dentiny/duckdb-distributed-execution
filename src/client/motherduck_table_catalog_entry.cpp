// Implementation note: most of the functions are delegated to parent class DuckTableEntry instead of taking a
// DuckTableEntry* entry. Because alter table would invalidate the pointer to the entry.

#include "motherduck_table_catalog_entry.hpp"

#include "distributed_alter_table.hpp"
#include "distributed_client.hpp"
#include "distributed_table_scan_function.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "logical_remote_alter_table.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_schema_catalog_entry.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

MotherduckTableCatalogEntry::MotherduckTableCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
                                                         DuckTableEntry *duck_table_entry_p,
                                                         unique_ptr<BoundCreateTableInfo> bound_create_table_info_p)
    : DuckTableEntry(motherduck_catalog_p, duck_table_entry_p->schema, *bound_create_table_info_p,
                     duck_table_entry_p->GetStorage().shared_from_this()),
      db_instance(db_instance_p), bound_create_table_info(std::move(bound_create_table_info_p)),
      motherduck_catalog_ref(motherduck_catalog_p) {
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::AlterEntry");
	return DuckTableEntry::AlterEntry(context, info);
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::AlterEntry (CatalogTransaction)");
	return DuckTableEntry::AlterEntry(std::move(transaction), info);
}

void MotherduckTableCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::UndoAlter");
	DuckTableEntry::UndoAlter(context, info);
}

void MotherduckTableCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Rollback");
	DuckTableEntry::Rollback(prev_entry);
}

void MotherduckTableCatalogEntry::OnDrop() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::OnDrop");
	DuckTableEntry::OnDrop();
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Copy");
	return DuckTableEntry::Copy(context);
}

unique_ptr<CreateInfo> MotherduckTableCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetInfo");
	return DuckTableEntry::GetInfo();
}

void MotherduckTableCatalogEntry::SetAsRoot() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::SetAsRoot");
	DuckTableEntry::SetAsRoot();
}

string MotherduckTableCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ToSQL");
	return DuckTableEntry::ToSQL();
}

Catalog &MotherduckTableCatalogEntry::ParentCatalog() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentCatalog");
	return motherduck_catalog_ref;
}

const Catalog &MotherduckTableCatalogEntry::ParentCatalog() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentCatalog (const)");
	return motherduck_catalog_ref;
}

SchemaCatalogEntry &MotherduckTableCatalogEntry::ParentSchema() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentSchema");
	return DuckTableEntry::ParentSchema();
}

const SchemaCatalogEntry &MotherduckTableCatalogEntry::ParentSchema() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentSchema (const)");
	return DuckTableEntry::ParentSchema();
}

void MotherduckTableCatalogEntry::Verify(Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Verify");
	DuckTableEntry::Verify(catalog);
}

unique_ptr<BaseStatistics> MotherduckTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetStatistics");
	return DuckTableEntry::GetStatistics(context, std::move(column_id));
}

TableFunction MotherduckTableCatalogEntry::GetScanFunction(ClientContext &context,
                                                           unique_ptr<FunctionData> &bind_data) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetScanFunction");

	// Attempt distributed execution for registered remote table.
	auto *md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&motherduck_catalog_ref);
	if (md_catalog_ptr && md_catalog_ptr->IsRemoteTable(name)) {
		auto config = md_catalog_ptr->GetRemoteTableConfig(name);
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Table query %s is distributed. Using remote scan from %s.",
		                                                 name, config.server_url));

		bind_data = make_uniq<DistributedTableScanBindData>(*this, config.server_url, config.remote_table_name);
		return DistributedTableScanFunction::GetFunction();
	}

	// Fallback to regular DuckDB scan for local tables.
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Table query %s is local.", name));
	return DuckTableEntry::GetScanFunction(context, bind_data);
}

TableFunction MotherduckTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                           const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetScanFunction");

	// Attempt distributed execution for registered remote table.
	auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&motherduck_catalog_ref);
	if (md_catalog_ptr && md_catalog_ptr->IsRemoteTable(name)) {
		auto config = md_catalog_ptr->GetRemoteTableConfig(name);
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Table query %s is distributed. Using remote scan from %s.",
		                                                 name, config.server_url));

		bind_data = make_uniq<DistributedTableScanBindData>(*this, config.server_url, config.remote_table_name);
		return DistributedTableScanFunction::GetFunction();
	}

	// Fallback to regular DuckDB scan for local tables.
	// Use the 2-parameter version which is inherited from DuckTableEntry
	return DuckTableEntry::GetScanFunction(context, bind_data);
}

bool MotherduckTableCatalogEntry::IsDuckTable() const {
	return DuckTableEntry::IsDuckTable();
}

TableStorageInfo MotherduckTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetStorageInfo");
	return DuckTableEntry::GetStorageInfo(context);
}

void MotherduckTableCatalogEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                                        LogicalUpdate &update, ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::BindUpdateConstraints");
	return DuckTableEntry::BindUpdateConstraints(binder, get, proj, update, context);
}

virtual_column_map_t MotherduckTableCatalogEntry::GetVirtualColumns() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetVirtualColumns");
	return DuckTableEntry::GetVirtualColumns();
}

vector<column_t> MotherduckTableCatalogEntry::GetRowIdColumns() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetRowIdColumns");
	return DuckTableEntry::GetRowIdColumns();
}

} // namespace duckdb
