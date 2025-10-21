#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_schema.hpp"
#include "motherduck_table.hpp"

namespace duckdb {

MotherduckSchema::MotherduckSchema(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

MotherduckSchema::~MotherduckSchema() = default;

void MotherduckSchema::Scan(ClientContext &context, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan not implemented");
}

void MotherduckSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("CreateFunction not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("CreateTable not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CreateView not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("CreateSequence not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateTableFunction(CatalogTransaction transaction,
                                                                 CreateTableFunctionInfo &info) {
	throw NotImplementedException("CreateTableFunction not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateCopyFunction(CatalogTransaction transaction,
                                                                CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CreateCopyFunction not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreatePragmaFunction(CatalogTransaction transaction,
                                                                  CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CreatePragmaFunction not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateCollation(CatalogTransaction transaction,
                                                             CreateCollationInfo &info) {
	throw NotImplementedException("CreateCollation not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CreateType not implemented");
}

optional_ptr<CatalogEntry> MotherduckSchema::LookupEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	lock_guard<mutex> guard(lock);
	auto &table_name = lookup_info.GetEntryName();
	if (auto it = tables.find(table_name); it != tables.end()) {
		return *it->second.get();
	}
	return nullptr;
}

void MotherduckSchema::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropEntry not implemented");
}

void MotherduckSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter not implemented");
}

} // namespace duckdb
