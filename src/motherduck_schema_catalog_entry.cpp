#include "motherduck_schema_catalog_entry.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

MotherduckSchemaCatalogEntry::MotherduckSchemaCatalogEntry(DatabaseInstance &db_instance_p,
                                                           SchemaCatalogEntry *schema_catalog_entry_p,
                                                           unique_ptr<CreateSchemaInfo> create_schema_info_p)
    : SchemaCatalogEntry(schema_catalog_entry_p->catalog, *create_schema_info_p), db_instance(db_instance_p),
      create_schema_info(std::move(create_schema_info_p)), schema_catalog_entry(schema_catalog_entry_p) {
}

unique_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::AlterEntry");
	return schema_catalog_entry->AlterEntry(context, info);
}

unique_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::AlterEntry (CatalogTransaction)");
	return schema_catalog_entry->AlterEntry(std::move(transaction), info);
}

void MotherduckSchemaCatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::UndoAlter");
	schema_catalog_entry->UndoAlter(context, info);
}

void MotherduckSchemaCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Rollback");
	schema_catalog_entry->Rollback(prev_entry);
}

void MotherduckSchemaCatalogEntry::OnDrop() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::OnDrop");
	schema_catalog_entry->OnDrop();
}

unique_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Copy");
	return schema_catalog_entry->Copy(context);
}

unique_ptr<CreateInfo> MotherduckSchemaCatalogEntry::GetInfo() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::GetInfo");
	return schema_catalog_entry->GetInfo();
}

void MotherduckSchemaCatalogEntry::SetAsRoot() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::SetAsRoot");
	schema_catalog_entry->SetAsRoot();
}

string MotherduckSchemaCatalogEntry::ToSQL() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::ToSQL");
	return schema_catalog_entry->ToSQL();
}

Catalog &MotherduckSchemaCatalogEntry::ParentCatalog() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::ParentCatalog");
	return schema_catalog_entry->ParentCatalog();
}

const Catalog &MotherduckSchemaCatalogEntry::ParentCatalog() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::ParentCatalog (const)");
	return schema_catalog_entry->ParentCatalog();
}

SchemaCatalogEntry &MotherduckSchemaCatalogEntry::ParentSchema() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::ParentSchema");
	return schema_catalog_entry->ParentSchema();
}

const SchemaCatalogEntry &MotherduckSchemaCatalogEntry::ParentSchema() const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::ParentSchema (const)");
	return schema_catalog_entry->ParentSchema();
}

void MotherduckSchemaCatalogEntry::Verify(Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Verify");
	schema_catalog_entry->Verify(catalog);
}

void MotherduckSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                        const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Scan (with ClientContext)");
	schema_catalog_entry->Scan(context, type, callback);
}

void MotherduckSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Scan");
	schema_catalog_entry->Scan(type, callback);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateIndex(CatalogTransaction transaction,
                                                                     CreateIndexInfo &info, TableCatalogEntry &table) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateIndex");
	return schema_catalog_entry->CreateIndex(std::move(transaction), info, table);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateFunction(CatalogTransaction transaction,
                                                                        CreateFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateFunction");
	return schema_catalog_entry->CreateFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateTable(CatalogTransaction transaction,
                                                                     BoundCreateTableInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateTable");
	return schema_catalog_entry->CreateTable(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateView(CatalogTransaction transaction,
                                                                    CreateViewInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateView");
	return schema_catalog_entry->CreateView(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateSequence(CatalogTransaction transaction,
                                                                        CreateSequenceInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateSequence");
	return schema_catalog_entry->CreateSequence(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                             CreateTableFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateTableFunction");
	return schema_catalog_entry->CreateTableFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                            CreateCopyFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateCopyFunction");
	return schema_catalog_entry->CreateCopyFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                              CreatePragmaFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreatePragmaFunction");
	return schema_catalog_entry->CreatePragmaFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateCollation(CatalogTransaction transaction,
                                                                         CreateCollationInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateCollation");
	return schema_catalog_entry->CreateCollation(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::CreateType(CatalogTransaction transaction,
                                                                    CreateTypeInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::CreateType");
	return schema_catalog_entry->CreateType(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                     const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::LookupEntry");
	return schema_catalog_entry->LookupEntry(std::move(transaction), lookup_info);
}

CatalogSet::EntryLookup MotherduckSchemaCatalogEntry::LookupEntryDetailed(CatalogTransaction transaction,
                                                                          const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::LookupEntryDetailed");
	return schema_catalog_entry->LookupEntryDetailed(std::move(transaction), lookup_info);
}

SimilarCatalogEntry MotherduckSchemaCatalogEntry::GetSimilarEntry(CatalogTransaction transaction,
                                                                  const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::GetSimilarEntry");
	return schema_catalog_entry->GetSimilarEntry(std::move(transaction), lookup_info);
}

void MotherduckSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::DropEntry");
	schema_catalog_entry->DropEntry(context, info);
}

void MotherduckSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Alter");
	schema_catalog_entry->Alter(std::move(transaction), info);
}

} // namespace duckdb
