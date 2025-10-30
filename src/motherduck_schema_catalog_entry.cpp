#include "motherduck_schema_catalog_entry.hpp"

#include "distributed_server.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "motherduck_catalog.hpp"

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
                                                           BaseQueryRecorder &query_recorder_p,
                                                           SchemaCatalogEntry *schema_catalog_entry_p,
                                                           unique_ptr<CreateSchemaInfo> create_schema_info_p)
    : DuckSchemaEntry(motherduck_catalog_p, *create_schema_info_p), db_instance(db_instance_p),
      query_recorder(query_recorder_p), create_schema_info(std::move(create_schema_info_p)),
      schema_catalog_entry(schema_catalog_entry_p), motherduck_catalog_ref(motherduck_catalog_p) {
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

		const auto query_recorder_handle = query_recorder.RecordQueryStart(create_sql);
		auto &server = DistributedServer::GetInstance();
		auto result = server.CreateTable(create_sql);
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

optional_ptr<CatalogEntry> MotherduckSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                     const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("MotherduckSchemaCatalogEntry::LookupEntry lookup entry %s with type %s",
	                                    lookup_info.GetEntryName(), CatalogTypeToString(lookup_info.GetCatalogType())));
	D_ASSERT(lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY);
	EntryLookupInfoKey key {
	    .type = lookup_info.GetCatalogType(),
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
	if (!catalog_entry) {
		return catalog_entry;
	}

	return WrapAndCacheTableCatalogEntryWithLock(std::move(key), catalog_entry.get());
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

	// Intercept remote table drop to propagate to the distributed server
	if (info.type == CatalogType::TABLE_ENTRY) {
		auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&motherduck_catalog_ref);
		if (md_catalog_ptr && md_catalog_ptr->IsRemoteTable(info.name)) {
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Drop remote table %s", info.name));

			string drop_sql = "DROP TABLE ";
			if (info.if_not_found != OnEntryNotFound::THROW_EXCEPTION) {
				drop_sql += "IF EXISTS ";
			}
			drop_sql += info.name;
			if (info.cascade) {
				drop_sql += " CASCADE";
			}

			const auto query_recorder_handle = query_recorder.RecordQueryStart(drop_sql);
			auto &server = DistributedServer::GetInstance();
			auto result = server.DropTable(drop_sql);
			if (result->HasError()) {
				throw Exception(ExceptionType::CATALOG, "Failed to drop remote table on server: " + result->GetError());
			}

			// Unregister after successful remote drop.
			md_catalog_ptr->UnregisterRemoteTable(info.name);
		}
		// Fallbacks to local table drop.
		else {
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Drop local table %s", info.name));
		}
	}

	// Local catalog entry is created for registered remote tables, which allows DuckDB to know the table existence and
	// its schema. All actual operations (i.e., scan, insert) will be intercepted and sent to server.
	//
	// TODO(hjiang): Check whether we could fake a remote table entry, which doesn't do ay IO operations.
	schema_catalog_entry->DropEntry(context, info);
}

void MotherduckSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckSchemaCatalogEntry::Alter");
	schema_catalog_entry->Alter(std::move(transaction), info);
}

} // namespace duckdb
