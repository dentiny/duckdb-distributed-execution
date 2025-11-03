#include "motherduck_schema_catalog_entry.hpp"

#include "distributed_client.hpp"
#include "distributed_protocol.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_index_catalog_entry.hpp"
#include "query_recorder_factory.hpp"

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

MotherduckSchemaCatalogEntry::MotherduckSchemaCatalogEntry(Catalog &motherduck_catalog_p,
                                                           DatabaseInstance &db_instance_p,
                                                           SchemaCatalogEntry *schema_catalog_entry_p,
                                                           unique_ptr<CreateSchemaInfo> create_schema_info_p)
    : DuckSchemaEntry(motherduck_catalog_p, *create_schema_info_p), db_instance(db_instance_p),
      create_schema_info(std::move(create_schema_info_p)), schema_catalog_entry(schema_catalog_entry_p),
      motherduck_catalog_ref(motherduck_catalog_p) {
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

	string index_name = info.index_name;
	string table_name = table.name;

	auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&motherduck_catalog_ref);
	const bool is_remote_table = md_catalog_ptr != nullptr && md_catalog_ptr->IsRemoteTable(table_name);

	if (is_remote_table) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Creating remote index %s on table %s", index_name, table_name));

		// For remote tables, create a stub catalog entry for DROP INDEX lookups.
		// MotherduckIndexCatalogEntry inherits from DuckIndexEntry with dummy storage infrastructure,
		// allowing CommitDrop() to be called safely as a no-op during transaction commit.
		info.dependencies.AddDependency(table);

		// Create a remote index catalog entry.
		auto remote_index = make_uniq<MotherduckIndexCatalogEntry>(motherduck_catalog_ref, *this, info);
		auto dependencies = remote_index->dependencies;
		auto *result = remote_index.get();

		// Add it to the catalog.
		auto *duck_schema = dynamic_cast<DuckSchemaEntry *>(schema_catalog_entry);
		if (duck_schema == nullptr) {
			throw InternalException("Expected schema catalog entry to be DuckSchemaEntry");
		}

		if (!duck_schema->AddEntryInternal(std::move(transaction), std::move(remote_index), info.on_conflict,
		                                   dependencies)) {
			return nullptr;
		}

		// Register as remote index for DROP INDEX tracking.
		md_catalog_ptr->RegisterRemoteIndex(index_name);

		return result;
	}

	// Fallback to local tables handling.
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Create local index %s on table %s", index_name, table_name));
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

	auto &create_info = info.Base();
	string table_name = create_info.table;

	auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&motherduck_catalog_ref);
	const bool is_remote = md_catalog_ptr && md_catalog_ptr->IsRemoteTable(table_name);
	if (is_remote) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Create remote table %s", table_name));

		// Generate CREATE TABLE SQL from info.
		string create_sql = "CREATE TABLE " + table_name + " (";
		for (idx_t i = 0; i < create_info.columns.LogicalColumnCount(); i++) {
			auto &col = create_info.columns.GetColumn(LogicalIndex(i));
			if (i > 0) {
				create_sql += ", ";
			}
			create_sql += col.Name() + " " + col.Type().ToString();
		}
		create_sql += ")";

		const auto query_recorder_handle = GetQueryRecorder().RecordQueryStart(create_sql);
		auto &client = DistributedClient::GetInstance();
		auto result = client.CreateTable(create_sql);
		if (result->HasError()) {
			throw Exception(ExceptionType::CATALOG, "Failed to create table on server: " + result->GetError());
		}
	} else {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Create local table %s", table_name));
	}

	// Create local catalog entry even for registered remote tables, which allows DuckDB to know the table existence and
	// its schema. All actual operations (i.e., scan, insert) will be intercepted and sent to server.
	//
	// TODO(hjiang): Check whether we could fake a remote table entry, which doesn't do ay IO operations.
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

CatalogEntry *MotherduckSchemaCatalogEntry::WrapAndCacheTableCatalogEntryWithLock(EntryLookupInfoKey key,
                                                                                  CatalogEntry *catalog_entry) {
	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	DuckTableEntry *table_catalog_entry = dynamic_cast<DuckTableEntry *>(catalog_entry);
	D_ASSERT(table_catalog_entry != nullptr);

	auto create_table_info = make_uniq<CreateTableInfo>();
	create_table_info->table = table_catalog_entry->name;
	create_table_info->columns = table_catalog_entry->GetColumns().Copy();
	create_table_info->constraints = CopyConstraints(table_catalog_entry->GetConstraints());
	create_table_info->temporary = table_catalog_entry->temporary;
	create_table_info->dependencies = table_catalog_entry->dependencies;
	create_table_info->comment = table_catalog_entry->comment;
	create_table_info->tags = table_catalog_entry->tags;

	auto bound_create_table_info = make_uniq<BoundCreateTableInfo>(*this, std::move(create_table_info));
	auto motherduck_table_catalog_entry = make_uniq<MotherduckTableCatalogEntry>(
	    catalog, db_instance, table_catalog_entry, std::move(bound_create_table_info));
	auto *ret = motherduck_table_catalog_entry.get();
	catalog_entries.emplace(std::move(key), std::move(motherduck_table_catalog_entry));
	return ret;
}

CatalogEntry *MotherduckSchemaCatalogEntry::WrapAndCacheIndexCatalogEntryWithLock(EntryLookupInfoKey key,
                                                                                  CatalogEntry *catalog_entry) {
	D_ASSERT(catalog_entry->type == CatalogType::INDEX_ENTRY);

	// Cache index entries are not cached, because they're already managed by DuckDB's catalog set.
	// TODO(hjiang): Check whether we could cache it as table catalog entries.
	return catalog_entry;
}

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                     const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("MotherduckSchemaCatalogEntry::LookupEntry lookup entry %s with type %s",
	                                    lookup_info.GetEntryName(), CatalogTypeToString(lookup_info.GetCatalogType())));

	auto catalog_type = lookup_info.GetCatalogType();
	EntryLookupInfoKey key {
	    .type = catalog_type,
	    .name = lookup_info.GetEntryName(),
	};

	std::lock_guard<std::mutex> lck(mu);
	auto iter = catalog_entries.find(key);
	if (iter != catalog_entries.end()) {
		DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::LookupEntry cache hit");
		return iter->second.get();
	}

	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::LookupEntry cache miss");
	auto catalog_entry = schema_catalog_entry->LookupEntry(std::move(transaction), lookup_info);
	if (catalog_entry == nullptr) {
		return catalog_entry;
	}

	// Wrap and cache based on the type.
	if (catalog_type == CatalogType::TABLE_ENTRY) {
		return WrapAndCacheTableCatalogEntryWithLock(std::move(key), catalog_entry.get());
	} else if (catalog_type == CatalogType::INDEX_ENTRY) {
		return WrapAndCacheIndexCatalogEntryWithLock(std::move(key), catalog_entry.get());
	}

	// TODO(hjiang): Wrap and cache other catalog types.
	return catalog_entry;
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

void MotherduckSchemaCatalogEntry::DropRemoteIndex(ClientContext &context, DropInfo &info,
                                                   MotherduckCatalog &md_catalog) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Dropping remote index: %s", info.name));

	string drop_sql = "DROP INDEX ";
	if (info.if_not_found != OnEntryNotFound::THROW_EXCEPTION) {
		drop_sql += "IF EXISTS ";
	}
	drop_sql += info.name;

	const auto query_recorder_handle = GetQueryRecorder().RecordQueryStart(drop_sql);
	auto &client = DistributedClient::GetInstance();
	auto result = client.ExecuteSQL(drop_sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::CATALOG, "Failed to drop remote index on server: " + result->GetError());
	}

	md_catalog.UnregisterRemoteIndex(info.name);
}

void MotherduckSchemaCatalogEntry::DropRemoteTable(ClientContext &context, DropInfo &info,
                                                   MotherduckCatalog &md_catalog) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Dropping remote table: %s", info.name));

	string drop_sql = "DROP TABLE ";
	if (info.if_not_found != OnEntryNotFound::THROW_EXCEPTION) {
		drop_sql += "IF EXISTS ";
	}
	drop_sql += info.name;
	if (info.cascade) {
		drop_sql += " CASCADE";
	}

	const auto query_recorder_handle = GetQueryRecorder().RecordQueryStart(drop_sql);
	auto &client = DistributedClient::GetInstance();
	auto result = client.ExecuteSQL(drop_sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::CATALOG, "Failed to drop remote table on server: " + result->GetError());
	}

	md_catalog.UnregisterRemoteTable(info.name);
}

void MotherduckSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("MotherduckSchemaCatalogEntry::DropEntry - type=%s name=%s",
	                                                 CatalogTypeToString(info.type), info.name));

	auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&motherduck_catalog_ref);

	// Handle remote index drops.
	if (info.type == CatalogType::INDEX_ENTRY && md_catalog_ptr != nullptr &&
	    md_catalog_ptr->IsRemoteIndex(info.name)) {
		DropRemoteIndex(context, info, *md_catalog_ptr);
	}

	// Handle remote table drops.
	else if (info.type == CatalogType::TABLE_ENTRY && md_catalog_ptr != nullptr &&
	         md_catalog_ptr->IsRemoteTable(info.name)) {
		DropRemoteTable(context, info, *md_catalog_ptr);
	}

	// For non-remote entries (or remote tables after remote drop), delegate to the underlying schema catalog.
	schema_catalog_entry->DropEntry(context, info);

	// Remove from cache after successful drop.
	EntryLookupInfoKey key {
	    .type = info.type,
	    .name = info.name,
	};
	std::lock_guard<std::mutex> lck(mu);
	// Here we don't check erase result since we haven't implemented all catalog entry types.
	catalog_entries.erase(key);
}

void MotherduckSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Alter");
	schema_catalog_entry->Alter(std::move(transaction), info);
}

} // namespace duckdb
