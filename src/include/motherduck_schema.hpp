#pragma once

#include <mutex>

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

// Forward declaration.
class CreateSchemaInfo;
class DuckSchemaEntry;
class DatabaseInstance;

class MotherduckSchemaEntry : public SchemaCatalogEntry {
public:
	MotherduckSchemaEntry(DatabaseInstance &db_instance_p, unique_ptr<CreateSchemaInfo> create_schema_info,
	                      SchemaCatalogEntry *schema_catalog_entry_p);

	~MotherduckSchemaEntry() override = default;

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;

	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;

	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;

	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;

	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;

	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;

	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;

	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;

	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	void DropEntry(ClientContext &context, DropInfo &info) override;

	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

	CatalogSet::EntryLookup LookupEntryDetailed(CatalogTransaction transaction,
	                                            const EntryLookupInfo &lookup_info) override;

	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	void Verify(Catalog &catalog) override;

private:
	CatalogEntry *WrapAndCacheTableCatalogEntryWithLock(string key, CatalogEntry *catalog_entry);
	CatalogEntry *WrapAndCacheSchemaCatalogEntryWithLock(string key, CatalogEntry *catalog_entry);

	DatabaseInstance &db_instance;
	unique_ptr<CreateSchemaInfo> create_schema_info;
	SchemaCatalogEntry *schema_catalog_entry;

	std::mutex mu;
	// TODO(hjiang): Use a better key.
	unordered_map<string, unique_ptr<CatalogEntry>> catalog_entries;
};

} // namespace duckdb
