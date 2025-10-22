#include "motherduck_schema.hpp"

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_table.hpp"

namespace duckdb {

MotherduckSchemaEntry::MotherduckSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), duckdb_schema_entry(make_uniq<DuckSchemaEntry>(catalog, info)) {
}

MotherduckSchemaEntry::~MotherduckSchemaEntry() = default;

void MotherduckSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	duckdb_schema_entry->Scan(context, type, callback);
}

void MotherduckSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	duckdb_schema_entry->Scan(type, callback);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                              TableCatalogEntry &table) {
	return duckdb_schema_entry->CreateIndex(std::move(transaction), info, table);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                 CreateFunctionInfo &info) {
	return duckdb_schema_entry->CreateFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	return duckdb_schema_entry->CreateTable(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	return duckdb_schema_entry->CreateView(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                 CreateSequenceInfo &info) {
	return duckdb_schema_entry->CreateSequence(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                      CreateTableFunctionInfo &info) {
	return duckdb_schema_entry->CreateTableFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                     CreateCopyFunctionInfo &info) {
	return duckdb_schema_entry->CreateCopyFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                       CreatePragmaFunctionInfo &info) {
	return duckdb_schema_entry->CreatePragmaFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                  CreateCollationInfo &info) {
	return duckdb_schema_entry->CreateCollation(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	return duckdb_schema_entry->CreateType(std::move(transaction), info);
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	return duckdb_schema_entry->LookupEntry(std::move(transaction), lookup_info);
}

void MotherduckSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	duckdb_schema_entry->DropEntry(context, info);
}

void MotherduckSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	duckdb_schema_entry->Alter(std::move(transaction), info);
}

CatalogSet::EntryLookup MotherduckSchemaEntry::LookupEntryDetailed(CatalogTransaction transaction,
                                                                   const EntryLookupInfo &lookup_info) {
	return duckdb_schema_entry->LookupEntryDetailed(std::move(transaction), lookup_info);
}

SimilarCatalogEntry MotherduckSchemaEntry::GetSimilarEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	return duckdb_schema_entry->GetSimilarEntry(std::move(transaction), lookup_info);
}

unique_ptr<CatalogEntry> MotherduckSchemaEntry::Copy(ClientContext &context) const {
	return duckdb_schema_entry->Copy(context);
}

void MotherduckSchemaEntry::Verify(Catalog &catalog) {
	duckdb_schema_entry->Verify(catalog);
}

} // namespace duckdb
