#include "motherduck_index_catalog_entry.hpp"

#include "duckdb/logging/logger.hpp"
#include "motherduck_catalog.hpp"

namespace duckdb {

MotherduckIndexCatalogEntry::MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
                                                         IndexCatalogEntry *duck_index_entry_p, CreateIndexInfo &info_p)
    : IndexCatalogEntry(motherduck_catalog_p, duck_index_entry_p->schema, info_p), db_instance(db_instance_p),
      duck_index_entry(duck_index_entry_p), motherduck_catalog_ref(motherduck_catalog_p) {
}

unique_ptr<CatalogEntry> MotherduckIndexCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::AlterEntry");
	return duck_index_entry->AlterEntry(context, info);
}

unique_ptr<CatalogEntry> MotherduckIndexCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::AlterEntry (CatalogTransaction)");
	return duck_index_entry->AlterEntry(std::move(transaction), info);
}

void MotherduckIndexCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::UndoAlter");
	duck_index_entry->UndoAlter(context, info);
}

unique_ptr<CatalogEntry> MotherduckIndexCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::Copy");
	return duck_index_entry->Copy(context);
}

unique_ptr<CreateInfo> MotherduckIndexCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::GetInfo");
	return duck_index_entry->GetInfo();
}

string MotherduckIndexCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::ToSQL");
	return duck_index_entry->ToSQL();
}

string MotherduckIndexCatalogEntry::GetSchemaName() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::GetSchemaName");
	return duck_index_entry->GetSchemaName();
}

string MotherduckIndexCatalogEntry::GetTableName() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckIndexCatalogEntry::GetTableName");
	return duck_index_entry->GetTableName();
}

} // namespace duckdb
