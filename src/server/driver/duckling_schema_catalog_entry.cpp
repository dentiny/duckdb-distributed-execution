#include "server/driver/duckling_schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "server/driver/duckling_catalog.hpp"
#include "server/driver/duckling_table_catalog_entry.hpp"
#include "server/driver/duckling_index_catalog_entry.hpp"

namespace duckdb {

DucklingSchemaCatalogEntry::DucklingSchemaCatalogEntry(Catalog &duckling_catalog_p, DatabaseInstance &db_instance_p,
                                                       SchemaCatalogEntry *schema_catalog_entry_p,
                                                       unique_ptr<CreateSchemaInfo> create_schema_info_p)
    : DuckSchemaEntry(duckling_catalog_p, *create_schema_info_p), db_instance(db_instance_p),
      create_schema_info(std::move(create_schema_info_p)), schema_catalog_entry(schema_catalog_entry_p),
      duckling_catalog_ref(duckling_catalog_p) {
}

unique_ptr<CatalogEntry> DucklingSchemaCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::AlterEntry");
	return schema_catalog_entry->AlterEntry(context, info);
}

unique_ptr<CatalogEntry> DucklingSchemaCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::AlterEntry (CatalogTransaction)");
	return schema_catalog_entry->AlterEntry(std::move(transaction), info);
}

void DucklingSchemaCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::UndoAlter");
	schema_catalog_entry->UndoAlter(context, info);
}

void DucklingSchemaCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::Rollback");
	schema_catalog_entry->Rollback(prev_entry);
}

void DucklingSchemaCatalogEntry::OnDrop() {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::OnDrop");
	schema_catalog_entry->OnDrop();
}

unique_ptr<CatalogEntry> DucklingSchemaCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::Copy");
	return schema_catalog_entry->Copy(context);
}

unique_ptr<CreateInfo> DucklingSchemaCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::GetInfo");
	return schema_catalog_entry->GetInfo();
}

void DucklingSchemaCatalogEntry::SetAsRoot() {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::SetAsRoot");
	schema_catalog_entry->SetAsRoot();
}

string DucklingSchemaCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::ToSQL");
	return schema_catalog_entry->ToSQL();
}

Catalog &DucklingSchemaCatalogEntry::ParentCatalog() {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::ParentCatalog");
	return schema_catalog_entry->ParentCatalog();
}

const Catalog &DucklingSchemaCatalogEntry::ParentCatalog() const {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::ParentCatalog (const)");
	return schema_catalog_entry->ParentCatalog();
}

SchemaCatalogEntry &DucklingSchemaCatalogEntry::ParentSchema() {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::ParentSchema");
	return schema_catalog_entry->ParentSchema();
}

const SchemaCatalogEntry &DucklingSchemaCatalogEntry::ParentSchema() const {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::ParentSchema (const)");
	return schema_catalog_entry->ParentSchema();
}

void DucklingSchemaCatalogEntry::Verify(Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::Verify");
	schema_catalog_entry->Verify(catalog);
}

void DucklingSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                      const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::Scan (with ClientContext)");
	schema_catalog_entry->Scan(context, type, callback);
}

void DucklingSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::Scan");
	schema_catalog_entry->Scan(type, callback);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateIndex(CatalogTransaction transaction,
                                                                   CreateIndexInfo &info, TableCatalogEntry &table) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateIndex");
	return schema_catalog_entry->CreateIndex(std::move(transaction), info, table);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateFunction(CatalogTransaction transaction,
                                                                      CreateFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateFunction");
	return schema_catalog_entry->CreateFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateTable(CatalogTransaction transaction,
                                                                   BoundCreateTableInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateTable");
	return schema_catalog_entry->CreateTable(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateView(CatalogTransaction transaction,
                                                                  CreateViewInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateView");
	return schema_catalog_entry->CreateView(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateSequence(CatalogTransaction transaction,
                                                                      CreateSequenceInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateSequence");
	return schema_catalog_entry->CreateSequence(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                           CreateTableFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateTableFunction");
	return schema_catalog_entry->CreateTableFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                          CreateCopyFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateCopyFunction");
	return schema_catalog_entry->CreateCopyFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                            CreatePragmaFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreatePragmaFunction");
	return schema_catalog_entry->CreatePragmaFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateCollation(CatalogTransaction transaction,
                                                                       CreateCollationInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateCollation");
	return schema_catalog_entry->CreateCollation(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::CreateType(CatalogTransaction transaction,
                                                                  CreateTypeInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingSchemaCatalogEntry::CreateType");
	return schema_catalog_entry->CreateType(std::move(transaction), info);
}

// TODO(hjiang): Implement table catalog entry cache.
CatalogEntry *DucklingSchemaCatalogEntry::WrapAndCacheTableCatalogEntryWithLock(string key,
                                                                                CatalogEntry *catalog_entry) {
	return catalog_entry;
}

// TODO(hjiang): Implement index catalog entry cache.
CatalogEntry *DucklingSchemaCatalogEntry::WrapAndCacheIndexCatalogEntryWithLock(string key,
                                                                                CatalogEntry *catalog_entry) {
	return catalog_entry;
}

optional_ptr<CatalogEntry> DucklingSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                   const EntryLookupInfo &lookup_info) {
	return schema_catalog_entry->LookupEntry(std::move(transaction), lookup_info);
}

void DucklingSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info) {
	schema_catalog_entry->DropEntry(context, info);
}

void DucklingSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	schema_catalog_entry->Alter(std::move(transaction), info);
}

} // namespace duckdb
