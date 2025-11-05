#include "duckling_schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckling_catalog.hpp"
#include <iostream>

namespace duckdb {

DucklingSchemaCatalogEntry::DucklingSchemaCatalogEntry(Catalog &duckling_catalog_p, DatabaseInstance &db_instance_p,
                                                       SchemaCatalogEntry *schema_catalog_entry_p,
                                                       unique_ptr<CreateSchemaInfo> create_schema_info_p)
    : DuckSchemaEntry(duckling_catalog_p, *create_schema_info_p), db_instance(db_instance_p),
      create_schema_info(std::move(create_schema_info_p)), schema_catalog_entry(schema_catalog_entry_p),
      duckling_catalog_ref(duckling_catalog_p) {
	std::cerr << "[DUCKLING SCHEMA] DucklingSchemaCatalogEntry created for schema: " 
	          << create_schema_info_p->schema << std::endl;
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

} // namespace duckdb

