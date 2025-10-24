#include "motherduck_schema.hpp"

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_catalog_entry.hpp"
#include "motherduck_table.hpp"

namespace duckdb {

MotherduckSchemaEntry::MotherduckSchemaEntry(DatabaseInstance &db_instance_p,
                                             SchemaCatalogEntry *schema_catalog_entry_p)
    : SchemaCatalogEntry(schema_catalog_entry_p->catalog, schema_catalog_entry_p->GetInfo()->Cast<CreateSchemaInfo>()),
      db_instance(db_instance_p), schema_catalog_entry(schema_catalog_entry_p) {
}

void MotherduckSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::Scan");
	schema_catalog_entry->Scan(context, type, callback);
}

void MotherduckSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::Scan");
	schema_catalog_entry->Scan(type, callback);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                              TableCatalogEntry &table) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateIndex");
	return schema_catalog_entry->CreateIndex(std::move(transaction), info, table);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                 CreateFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateFunction");
	return schema_catalog_entry->CreateFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	auto create_table_str = info.base->ToString();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("MotherduckSchemaEntry::CreateTable %s", create_table_str));

	auto duck_catalog_entry = schema_catalog_entry->CreateTable(std::move(transaction), info);
	return duck_catalog_entry;
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateView");
	return schema_catalog_entry->CreateView(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                 CreateSequenceInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateSequence");
	return schema_catalog_entry->CreateSequence(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                      CreateTableFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateTableFunction");
	return schema_catalog_entry->CreateTableFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                     CreateCopyFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateCopyFunction");
	return schema_catalog_entry->CreateCopyFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                       CreatePragmaFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreatePragmaFunction");
	return schema_catalog_entry->CreatePragmaFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                  CreateCollationInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateCollation");
	return schema_catalog_entry->CreateCollation(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::CreateType");
	return schema_catalog_entry->CreateType(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	auto lookup_entry = lookup_info.GetEntryName();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("MotherduckSchemaEntry::LookupEntry %s", lookup_entry));

	// TODO(hjiang): Cache motherduck catalog entry, but it segfaults if we wrap it.
	auto duck_catalog_entry = schema_catalog_entry->LookupEntry(std::move(transaction), lookup_info);
	return duck_catalog_entry;
}

void MotherduckSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::DropEntry");
	schema_catalog_entry->DropEntry(context, info);
}

void MotherduckSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::Alter");
	schema_catalog_entry->Alter(std::move(transaction), info);
}

CatalogSet::EntryLookup MotherduckSchemaEntry::LookupEntryDetailed(CatalogTransaction transaction,
                                                                   const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::LookupEntryDetailed");
	return schema_catalog_entry->LookupEntryDetailed(std::move(transaction), lookup_info);
}

SimilarCatalogEntry MotherduckSchemaEntry::GetSimilarEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::GetSimilarEntry");
	return schema_catalog_entry->GetSimilarEntry(std::move(transaction), lookup_info);
}

unique_ptr<CatalogEntry> MotherduckSchemaEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaEntry::Copy");
	return schema_catalog_entry->Copy(context);
}

void MotherduckSchemaEntry::Verify(Catalog &catalog) {
	schema_catalog_entry->Verify(catalog);
}

} // namespace duckdb
