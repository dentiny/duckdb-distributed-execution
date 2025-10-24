#include "motherduck_catalog_entry.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

MotherduckCatalogEntry::MotherduckCatalogEntry(DatabaseInstance &db_instance_p, CatalogEntry *catalog_entry_p)
    : CatalogEntry(catalog_entry_p->type, catalog_entry_p->name, catalog_entry_p->oid), db_instance(db_instance_p),
      catalog_entry(catalog_entry_p) {
}

unique_ptr<CatalogEntry> MotherduckCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::AlterEntry");
	return catalog_entry->AlterEntry(context, info);
}

unique_ptr<CatalogEntry> MotherduckCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::AlterEntry (CatalogTransaction)");
	return catalog_entry->AlterEntry(std::move(transaction), info);
}

void MotherduckCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::UndoAlter");
	catalog_entry->UndoAlter(context, info);
}

void MotherduckCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::Rollback");
	catalog_entry->Rollback(prev_entry);
}

void MotherduckCatalogEntry::OnDrop() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::OnDrop");
	catalog_entry->OnDrop();
}

unique_ptr<CatalogEntry> MotherduckCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::Copy");
	return catalog_entry->Copy(context);
}

unique_ptr<CreateInfo> MotherduckCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::GetInfo");
	return catalog_entry->GetInfo();
}

void MotherduckCatalogEntry::SetAsRoot() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::SetAsRoot");
	catalog_entry->SetAsRoot();
}

string MotherduckCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::ToSQL");
	return catalog_entry->ToSQL();
}

Catalog &MotherduckCatalogEntry::ParentCatalog() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::ParentCatalog");
	return catalog_entry->ParentCatalog();
}

const Catalog &MotherduckCatalogEntry::ParentCatalog() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::ParentCatalog (const)");
	return catalog_entry->ParentCatalog();
}

SchemaCatalogEntry &MotherduckCatalogEntry::ParentSchema() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::ParentSchema");
	return catalog_entry->ParentSchema();
}

const SchemaCatalogEntry &MotherduckCatalogEntry::ParentSchema() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::ParentSchema (const)");
	return catalog_entry->ParentSchema();
}

void MotherduckCatalogEntry::Verify(Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalogEntry::Verify");
	catalog_entry->Verify(catalog);
}

} // namespace duckdb
