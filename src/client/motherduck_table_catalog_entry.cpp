
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

namespace duckdb {

namespace {
// Helper function to generate ALTER TABLE SQL (duplicated from motherduck_schema_catalog_entry.cpp)
string GenerateAlterTableSQL(AlterTableInfo &info, const string &table_name) {
	string sql = "ALTER TABLE " + table_name + " ";
	
	switch (info.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add_info = info.Cast<AddColumnInfo>();
		sql += "ADD COLUMN ";
		if (add_info.if_column_not_exists) {
			sql += "IF NOT EXISTS ";
		}
		sql += add_info.new_column.Name() + " " + add_info.new_column.Type().ToString();
		if (add_info.new_column.HasDefaultValue()) {
			sql += " DEFAULT " + add_info.new_column.DefaultValue().ToString();
		}
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = info.Cast<RemoveColumnInfo>();
		sql += "DROP COLUMN ";
		if (remove_info.if_column_exists) {
			sql += "IF EXISTS ";
		}
		sql += remove_info.removed_column;
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename_info = info.Cast<RenameColumnInfo>();
		sql += "RENAME COLUMN " + rename_info.old_name + " TO " + rename_info.new_name;
		break;
	}
	case AlterTableType::RENAME_TABLE: {
		auto &rename_info = info.Cast<RenameTableInfo>();
		sql = "ALTER TABLE " + table_name + " RENAME TO " + rename_info.new_table_name;
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_info = info.Cast<ChangeColumnTypeInfo>();
		sql += "ALTER COLUMN " + change_info.column_name + " TYPE " + change_info.target_type.ToString();
		break;
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set_default_info = info.Cast<SetDefaultInfo>();
		sql += "ALTER COLUMN " + set_default_info.column_name + " SET DEFAULT " + set_default_info.expression->ToString();
		break;
	}
	case AlterTableType::SET_NOT_NULL: {
		auto &set_not_null_info = info.Cast<SetNotNullInfo>();
		sql += "ALTER COLUMN " + set_not_null_info.column_name + " SET NOT NULL";
		break;
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop_not_null_info = info.Cast<DropNotNullInfo>();
		sql += "ALTER COLUMN " + drop_not_null_info.column_name + " DROP NOT NULL";
		break;
	}
	default:
		throw NotImplementedException("Unsupported ALTER TABLE type for remote execution");
	}
	
	return sql;
}
} // namespace

MotherduckTableCatalogEntry::MotherduckTableCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
                                                         DuckTableEntry *duck_table_entry_p,
                                                         unique_ptr<BoundCreateTableInfo> bound_create_table_info_p)
    : DuckTableEntry(motherduck_catalog_p, duck_table_entry_p->schema, *bound_create_table_info_p,
                     duck_table_entry_p->GetStorage().shared_from_this()),
      db_instance(db_instance_p), bound_create_table_info(std::move(bound_create_table_info_p)),
      duck_table_entry(duck_table_entry_p), motherduck_catalog_ref(motherduck_catalog_p) {
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::AlterEntry");
	
	// AlterEntry is not called for remote tables - ALTER operations are intercepted
	// at the schema level in MotherduckSchemaCatalogEntry::Alter()
	return duck_table_entry->AlterEntry(context, info);
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::AlterEntry (CatalogTransaction)");
	
	// AlterEntry is not called for remote tables - ALTER operations are intercepted
	// at the schema level in MotherduckSchemaCatalogEntry::Alter()
	return duck_table_entry->AlterEntry(std::move(transaction), info);
}

void MotherduckTableCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::UndoAlter");
	duck_table_entry->UndoAlter(context, info);
}

void MotherduckTableCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Rollback");
	duck_table_entry->Rollback(prev_entry);
}

void MotherduckTableCatalogEntry::OnDrop() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::OnDrop");
	duck_table_entry->OnDrop();
}

unique_ptr<CatalogEntry> MotherduckTableCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Copy");
	return duck_table_entry->Copy(context);
}

unique_ptr<CreateInfo> MotherduckTableCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetInfo");
	return duck_table_entry->GetInfo();
}

void MotherduckTableCatalogEntry::SetAsRoot() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::SetAsRoot");
	duck_table_entry->SetAsRoot();
}

string MotherduckTableCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ToSQL");
	return duck_table_entry->ToSQL();
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
	return duck_table_entry->ParentSchema();
}

const SchemaCatalogEntry &MotherduckTableCatalogEntry::ParentSchema() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::ParentSchema (const)");
	return duck_table_entry->ParentSchema();
}

void MotherduckTableCatalogEntry::Verify(Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::Verify");
	duck_table_entry->Verify(catalog);
}

unique_ptr<BaseStatistics> MotherduckTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetStatistics");
	return duck_table_entry->GetStatistics(context, std::move(column_id));
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
	return duck_table_entry->GetScanFunction(context, bind_data);
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
	return duck_table_entry->GetScanFunction(context, bind_data);
}

bool MotherduckTableCatalogEntry::IsDuckTable() const {
	return duck_table_entry->IsDuckTable();
}

TableStorageInfo MotherduckTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetStorageInfo");
	return duck_table_entry->GetStorageInfo(context);
}

void MotherduckTableCatalogEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                                        LogicalUpdate &update, ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::BindUpdateConstraints");
	return duck_table_entry->BindUpdateConstraints(binder, get, proj, update, context);
}

virtual_column_map_t MotherduckTableCatalogEntry::GetVirtualColumns() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetVirtualColumns");
	// Don't use duck_table_entry as it may be stale after ALTER TABLE
	// Use the parent class implementation instead
	return DuckTableEntry::GetVirtualColumns();
}

vector<column_t> MotherduckTableCatalogEntry::GetRowIdColumns() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckTableCatalogEntry::GetRowIdColumns");
	// Don't use duck_table_entry as it may be stale after ALTER TABLE
	// Use the parent class implementation instead
	return DuckTableEntry::GetRowIdColumns();
}

} // namespace duckdb
