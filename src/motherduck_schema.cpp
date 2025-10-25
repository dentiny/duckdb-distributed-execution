#include "motherduck_schema.hpp"

#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_table.hpp"
#include "motherduck_schema_catalog_entry.hpp"
#include "motherduck_table_catalog_entry.hpp"

namespace duckdb {

namespace {
vector<unique_ptr<Constraint>> CopyConstraints(const vector<unique_ptr<Constraint>> &constraints) {
	vector<unique_ptr<Constraint>> res;
	res.reserve(constraints.size());
	for (const auto &cur_constraint : constraints) {
		res.emplace_back(cur_constraint->Copy());
	}
	return res;
}
} // namespace

MotherduckSchemaEntry::MotherduckSchemaEntry(DatabaseInstance &db_instance_p,
                                             unique_ptr<CreateSchemaInfo> create_schema_info_p,
                                             SchemaCatalogEntry *schema_catalog_entry_p)
    : SchemaCatalogEntry(schema_catalog_entry_p->catalog, *create_schema_info_p), db_instance(db_instance_p),
      create_schema_info(std::move(create_schema_info_p)), schema_catalog_entry(schema_catalog_entry_p) {
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

CatalogEntry *MotherduckSchemaEntry::WrapAndCacheTableCatalogEntryWithLock(string key, CatalogEntry *catalog_entry) {
	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	TableCatalogEntry *table_catalog_entry = dynamic_cast<TableCatalogEntry *>(catalog_entry);
	D_ASSERT(table_catalog_entry != nullptr);

	auto create_table_info = make_uniq<CreateTableInfo>();
	create_table_info->table = table_catalog_entry->name;
	create_table_info->columns = table_catalog_entry->GetColumns().Copy();
	create_table_info->constraints = CopyConstraints(table_catalog_entry->GetConstraints());
	create_table_info->temporary = table_catalog_entry->temporary;
	create_table_info->dependencies = table_catalog_entry->dependencies;
	create_table_info->comment = table_catalog_entry->comment;
	create_table_info->tags = table_catalog_entry->tags;

	auto motherduck_table_catalog_entry =
	    make_uniq<MotherduckTableCatalogEntry>(db_instance, table_catalog_entry, std::move(create_table_info));
	auto *ret = motherduck_table_catalog_entry.get();
	catalog_entries.emplace(std::move(key), std::move(motherduck_table_catalog_entry));
	return ret;
}
CatalogEntry *MotherduckSchemaEntry::WrapAndCacheSchemaCatalogEntryWithLock(string key, CatalogEntry *catalog_entry) {
	D_ASSERT(catalog_entry->type == CatalogType::SCHEMA_ENTRY);
	SchemaCatalogEntry *schema_catalog_entry = dynamic_cast<SchemaCatalogEntry *>(catalog_entry);
	D_ASSERT(schema_catalog_entry != nullptr);

	auto create_schema_info = make_uniq<CreateSchemaInfo>();
	auto motherduck_table_catalog_entry =
	    make_uniq<MotherduckSchemaCatalogEntry>(db_instance, schema_catalog_entry, std::move(create_schema_info));
	auto *ret = motherduck_table_catalog_entry.get();
	catalog_entries.emplace(std::move(key), std::move(motherduck_table_catalog_entry));
	return ret;
}

optional_ptr<CatalogEntry> MotherduckSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	auto lookup_entry = lookup_info.GetEntryName();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("MotherduckSchemaEntry::LookupEntry %s", lookup_entry));

	std::lock_guard<std::mutex> lck(mu);
	auto iter = catalog_entries.find(lookup_entry);
	if (iter != catalog_entries.end()) {
		return iter->second.get();
	}

	auto duck_catalog_entry = schema_catalog_entry->LookupEntry(std::move(transaction), lookup_info);
	if (!duck_catalog_entry) {
		return duck_catalog_entry;
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Create new entry with type %s",
	                                                 CatalogTypeToString(duck_catalog_entry->type)));
	if (duck_catalog_entry->type == CatalogType::TABLE_ENTRY) {
		return WrapAndCacheTableCatalogEntryWithLock(std::move(lookup_entry), duck_catalog_entry.get());
	}
	if (duck_catalog_entry->type == CatalogType::SCHEMA_ENTRY) {
		return WrapAndCacheSchemaCatalogEntryWithLock(std::move(lookup_entry), duck_catalog_entry.get());
	}

	throw InternalException("Unimplemented catalog entry type");
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
