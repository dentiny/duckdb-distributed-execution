#pragma once

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "entry_lookup_info_hash_utils.hpp"
#include "utils/mutex.hpp"

namespace duckdb {

// Forward declaration.
struct CreateSchemaInfo;
class DatabaseInstance;
class DuckherderCatalog;

class DuckherderSchemaCatalogEntry : public DuckSchemaEntry {
public:
	~DuckherderSchemaCatalogEntry() override = default;

	//===--------------------------------------------------------------------===//
	// CatalogEntry-specific functions
	//===--------------------------------------------------------------------===//
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	//===--------------------------------------------------------------------===//
	// SchemaCatalogEntry-specific functions
	//===--------------------------------------------------------------------===//
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	friend class DuckherderCatalog;

	DuckherderSchemaCatalogEntry(DuckherderCatalog &duckherder_catalog_p, DatabaseInstance &db_instance_p,
	                             CreateSchemaInfo &create_schema_info);

	optional_ptr<CatalogEntry> CreateTableLocal(CatalogTransaction transaction, BoundCreateTableInfo &info);
	optional_ptr<CatalogEntry> CreateTypeLocal(CatalogTransaction transaction, CreateTypeInfo &info);

	CatalogEntry *WrapAndCacheTableCatalogEntryWithLock(EntryLookupInfoKey key, CatalogEntry *catalog_entry)
	    DUCKDB_REQUIRES(mu);

	void DropRemoteIndex(ClientContext &context, const DropInfo &info);
	void DropRemoteTable(ClientContext &context, const DropInfo &info);
	// Drops the view on the control node before removing its local metadata.
	void DropRemoteView(ClientContext &context, const DropInfo &info);

	DatabaseInstance &db_instance;
	DuckherderCatalog &duckherder_catalog;

	struct CachedCatalogEntry {
		const CatalogEntry *source;
		optional_idx oid;
		transaction_t timestamp;
		unique_ptr<CatalogEntry> wrapper;
	};

	concurrency::mutex mu;
	// Cache for catalog entries, including table entries.
	unordered_map<EntryLookupInfoKey, CachedCatalogEntry, EntryLookupInfoHash, EntryLookupInfoEqual>
	    catalog_entries DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb
