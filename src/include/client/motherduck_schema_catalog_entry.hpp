#pragma once

#include <mutex>

#include "base_query_recorder.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "entry_lookup_info_hash_utils.hpp"

// TODO(hjiang): Likely will switch to forward declaration.
#include "motherduck_table_catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
class CreateSchemaInfo;
class DatabaseInstance;
class MotherduckCatalog;

class MotherduckSchemaCatalogEntry : public DuckSchemaEntry {
public:
	MotherduckSchemaCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
	                             SchemaCatalogEntry *table_catalog_entry_p,
	                             unique_ptr<CreateSchemaInfo> create_schema_info_p);
	~MotherduckSchemaCatalogEntry() override = default;

	//===--------------------------------------------------------------------===//
	// CatalogEntry-specific functions
	//===--------------------------------------------------------------------===//
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
	void UndoAlter(ClientContext &context, AlterInfo &info) override;
	void Rollback(CatalogEntry &prev_entry) override;
	void OnDrop() override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CreateInfo> GetInfo() const override;
	void SetAsRoot() override;
	string ToSQL() const override;

	Catalog &ParentCatalog() override;
	const Catalog &ParentCatalog() const override;
	SchemaCatalogEntry &ParentSchema() override;
	const SchemaCatalogEntry &ParentSchema() const override;

	void Verify(Catalog &catalog) override;

	//===--------------------------------------------------------------------===//
	// SchemaCatalogEntry-specific functions
	//===--------------------------------------------------------------------===//
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback);
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback);
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table);
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info);
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info);
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info);
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info);
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info);
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info);
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info);
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info);
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info);
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info);
	CatalogSet::EntryLookup LookupEntryDetailed(CatalogTransaction transaction, const EntryLookupInfo &lookup_info);
	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info);
	void DropEntry(ClientContext &context, DropInfo &info);
	void Alter(CatalogTransaction transaction, AlterInfo &info);

private:
	CatalogEntry *WrapAndCacheTableCatalogEntryWithLock(EntryLookupInfoKey key, CatalogEntry *catalog_entry);
	CatalogEntry *WrapAndCacheIndexCatalogEntryWithLock(EntryLookupInfoKey key, CatalogEntry *catalog_entry);

	void DropRemoteIndex(ClientContext &context, DropInfo &info, MotherduckCatalog &md_catalog);
	void DropRemoteTable(ClientContext &context, DropInfo &info, MotherduckCatalog &md_catalog);

	DatabaseInstance &db_instance;
	unique_ptr<CreateSchemaInfo> create_schema_info;
	SchemaCatalogEntry *schema_catalog_entry;
	// Direct reference to MotherduckCatalog.
	Catalog &motherduck_catalog_ref;

	std::mutex mu;
	// Cache for catalog entries, including table entries.
	unordered_map<EntryLookupInfoKey, unique_ptr<CatalogEntry>, EntryLookupInfoHash, EntryLookupInfoEqual>
	    catalog_entries;
};

} // namespace duckdb
