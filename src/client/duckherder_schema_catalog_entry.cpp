#include "duckherder_schema_catalog_entry.hpp"

#include "client/execution/distributed_client.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_extension_instance_state.hpp"
#include "duckherder_index_catalog_entry.hpp"
#include "duckherder_table_catalog_entry.hpp"
#include "utils/catalog_utils.hpp"

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

DuckherderSchemaCatalogEntry::DuckherderSchemaCatalogEntry(DuckherderCatalog &duckherder_catalog_p,
                                                           DatabaseInstance &db_instance_p,
                                                           CreateSchemaInfo &create_schema_info)
    : DuckSchemaEntry(duckherder_catalog_p, create_schema_info), db_instance(db_instance_p),
      duckherder_catalog(duckherder_catalog_p) {
}

unique_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::Copy(ClientContext &context) const {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::Copy");
	throw NotImplementedException("Altering Duckherder schemas is not supported");
}

void DuckherderSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                        const std::function<void(CatalogEntry &)> &callback) {
	DuckSchemaEntry::Scan(context, type, [&](CatalogEntry &entry) {
		if (entry.type != CatalogType::TABLE_ENTRY) {
			callback(entry);
			return;
		}
		EntryLookupInfoKey key {
		    .type = CatalogType::TABLE_ENTRY,
		    .name = entry.name,
		};
		CatalogEntry *wrapped_entry = nullptr;
		{
			concurrency::lock_guard<concurrency::mutex> lck(mu);
			wrapped_entry = WrapAndCacheTableCatalogEntryWithLock(std::move(key), &entry);
		}
		callback(*wrapped_entry);
	});
}

void DuckherderSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	DuckSchemaEntry::Scan(type, [&](CatalogEntry &entry) {
		if (entry.type != CatalogType::TABLE_ENTRY) {
			callback(entry);
			return;
		}
		EntryLookupInfoKey key {
		    .type = CatalogType::TABLE_ENTRY,
		    .name = entry.name,
		};
		CatalogEntry *wrapped_entry = nullptr;
		{
			concurrency::lock_guard<concurrency::mutex> lck(mu);
			wrapped_entry = WrapAndCacheTableCatalogEntryWithLock(std::move(key), &entry);
		}
		callback(*wrapped_entry);
	});
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateIndex(CatalogTransaction transaction,
                                                                     CreateIndexInfo &info, TableCatalogEntry &table) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::CreateIndex");

	string index_name = info.index_name;
	string table_name = table.name;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Creating remote index %s on table %s", index_name, table_name));

	// Remote indexes use a metadata-only local entry so DROP INDEX can bind without local table storage.
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table_name);
	auto local_table = DuckSchemaEntry::LookupEntry(transaction, table_lookup);
	if (!local_table) {
		throw InternalException("Local table metadata %s.%s is missing", name, table_name);
	}
	LogicalDependencyList table_dependencies;
	table_dependencies.AddDependency(*local_table);
	info.dependencies = std::move(table_dependencies);
	auto remote_index = make_uniq<DuckherderIndexCatalogEntry>(duckherder_catalog, *this, info);
	auto dependencies = remote_index->dependencies;
	auto *result = remote_index.get();
	if (!AddEntryInternal(std::move(transaction), std::move(remote_index), info.on_conflict, dependencies)) {
		return nullptr;
	}
	remote_indexes.Add(table, info);
	return result;
}

vector<IndexInfo> DuckherderSchemaCatalogEntry::GetRemoteIndexes(const string &table_name) {
	return remote_indexes.Get(table_name);
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateFunction(CatalogTransaction transaction,
                                                                        CreateFunctionInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::CreateFunction");
	if (info.internal) {
		return DuckSchemaEntry::CreateFunction(std::move(transaction), info);
	}
	if (!transaction.HasContext()) {
		throw InternalException("Cannot create a remote Duckherder function without a client context");
	}

	// Create macro and table macro.
	if (info.type == CatalogType::MACRO_ENTRY || info.type == CatalogType::TABLE_MACRO_ENTRY) {
		auto &macro_info = info.Cast<CreateMacroInfo>();
		auto result =
		    duckherder_catalog.GetClient(transaction.GetContext())
		        .ExecuteStatement(macro_info.ToString(), StatementType::CREATE_STATEMENT, duckherder_catalog.GetName());
		if (result->HasError()) {
			throw CatalogException("Failed to create macro on server: %s", result->GetError());
		}
	}

	return DuckSchemaEntry::CreateFunction(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateTable(CatalogTransaction transaction,
                                                                     BoundCreateTableInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::CreateTable");

	auto &create_info = info.Base();
	if (create_info.internal) {
		return DuckSchemaEntry::CreateTable(std::move(transaction), info);
	}
	if (!transaction.HasContext()) {
		throw InternalException("Cannot create a remote Duckherder table without a client context");
	}
	string table_name = create_info.table;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Create remote table %s", table_name));

	// Preserve the complete CREATE TABLE definition, including constraints, defaults, and generated columns.
	auto create_sql = create_info.ToString();
	auto &instance_state = GetDuckherderInstanceStateOrThrow(db_instance);
	const auto query_recorder_handle = instance_state.GetQueryRecorder()->RecordQueryStart(create_sql);
	auto &client = duckherder_catalog.GetClient(transaction.GetContext());
	auto result = client.ExecuteStatement(create_sql, StatementType::CREATE_STATEMENT, duckherder_catalog.GetName());
	if (result->HasError()) {
		throw CatalogException("Failed to create table on server: %s", result->GetError());
	}
	// The local entry is only the in-memory metadata cache used for binding.
	return DuckSchemaEntry::CreateTable(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateView(CatalogTransaction transaction,
                                                                    CreateViewInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::CreateView");
	if (info.internal) {
		return DuckSchemaEntry::CreateView(std::move(transaction), info);
	}
	if (!transaction.HasContext()) {
		throw InternalException("Cannot create a remote Duckherder view without a client context");
	}

	auto result = duckherder_catalog.GetClient(transaction.GetContext())
	                  .ExecuteStatement(info.ToString(), StatementType::CREATE_STATEMENT, duckherder_catalog.GetName());
	if (result->HasError()) {
		throw CatalogException("Failed to create view on server: %s", result->GetError());
	}
	return DuckSchemaEntry::CreateView(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw CatalogException("CREATE SEQUENCE is not supported by Duckherder catalogs");
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateType(CatalogTransaction transaction,
                                                                    CreateTypeInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::CreateType");
	if (info.internal) {
		return DuckSchemaEntry::CreateType(std::move(transaction), info);
	}
	if (!transaction.HasContext()) {
		throw InternalException("Cannot create a remote Duckherder type without a client context");
	}

	// The named type must exist on the Control Node because complete remote statements are parsed and executed there.
	// Keep the local catalog entry as metadata for binding subsequent client statements.
	auto result = duckherder_catalog.GetClient(transaction.GetContext())
	                  .ExecuteStatement(info.ToString(), StatementType::CREATE_STATEMENT, duckherder_catalog.GetName());
	if (result->HasError()) {
		throw CatalogException("Failed to create type on server: %s", result->GetError());
	}

	return DuckSchemaEntry::CreateType(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateTableLocal(CatalogTransaction transaction,
                                                                          BoundCreateTableInfo &info) {
	return DuckSchemaEntry::CreateTable(std::move(transaction), info);
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::CreateTypeLocal(CatalogTransaction transaction,
                                                                         CreateTypeInfo &info) {
	return DuckSchemaEntry::CreateType(std::move(transaction), info);
}

CatalogEntry *DuckherderSchemaCatalogEntry::WrapAndCacheTableCatalogEntryWithLock(EntryLookupInfoKey key,
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
	auto duckherder_table_catalog_entry = unique_ptr<DuckherderTableCatalogEntry>(new DuckherderTableCatalogEntry(
	    duckherder_catalog, db_instance, table_catalog_entry, std::move(bound_create_table_info)));
	// The wrapper represents the same catalog object and must preserve its identity and owning set for dependency
	// validation and cascading drops.
	duckherder_table_catalog_entry->oid = table_catalog_entry->oid;
	duckherder_table_catalog_entry->set = table_catalog_entry->set;
	auto *ret = duckherder_table_catalog_entry.get();
	catalog_entries.erase(key);
	catalog_entries.emplace(std::move(key),
	                        CachedCatalogEntry {catalog_entry, catalog_entry->oid, catalog_entry->timestamp.load(),
	                                            std::move(duckherder_table_catalog_entry)});
	return ret;
}

optional_ptr<CatalogEntry> DuckherderSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                     const EntryLookupInfo &lookup_info) {
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("DuckherderSchemaCatalogEntry::LookupEntry lookup entry %s with type %s",
	                                    lookup_info.GetEntryName(), CatalogTypeToString(lookup_info.GetCatalogType())));

	auto catalog_type = lookup_info.GetCatalogType();
	EntryLookupInfoKey key {
	    .type = catalog_type,
	    .name = lookup_info.GetEntryName(),
	};

	auto catalog_entry = DuckSchemaEntry::LookupEntry(std::move(transaction), lookup_info);
	if (catalog_entry == nullptr) {
		return catalog_entry;
	}

	if (catalog_entry->type == CatalogType::TABLE_ENTRY) {
		concurrency::lock_guard<concurrency::mutex> lck(mu);
		auto iter = catalog_entries.find(key);
		if (iter != catalog_entries.end() && iter->second.source == catalog_entry.get() &&
		    iter->second.oid == catalog_entry->oid && iter->second.timestamp == catalog_entry->timestamp.load()) {
			DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::LookupEntry cache hit");
			return iter->second.wrapper.get();
		}
		DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::LookupEntry cache miss");
		return WrapAndCacheTableCatalogEntryWithLock(std::move(key), catalog_entry.get());
	}

	// TODO(hjiang): Wrap and cache other catalog types.
	return catalog_entry;
}

void DuckherderSchemaCatalogEntry::DropRemoteIndex(ClientContext &context, const DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Dropping remote index: %s", info.name));

	auto schema_name = KeywordHelper::WriteQuoted(name, '"');
	auto index_name = KeywordHelper::WriteQuoted(info.name, '"');
	auto if_exists = info.if_not_found == OnEntryNotFound::THROW_EXCEPTION ? "" : "IF EXISTS ";
	auto drop_sql = StringUtil::Format("DROP INDEX %s%s.%s", if_exists, schema_name, index_name);

	auto &instance_state = GetDuckherderInstanceStateOrThrow(db_instance);
	const auto query_recorder_handle = instance_state.GetQueryRecorder()->RecordQueryStart(drop_sql);
	auto &client = duckherder_catalog.GetClient(context);
	auto result = client.ExecuteStatement(drop_sql, StatementType::DROP_STATEMENT);
	if (result->HasError()) {
		throw CatalogException("Failed to drop remote index on server: %s", result->GetError());
	}
	remote_indexes.RemoveIndex(info.name);
}

void DuckherderSchemaCatalogEntry::DropRemoteTable(ClientContext &context, const DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Dropping remote table: %s", info.name));

	auto schema_name = KeywordHelper::WriteQuoted(name, '"');
	auto table_name = KeywordHelper::WriteQuoted(info.name, '"');
	auto if_exists = info.if_not_found == OnEntryNotFound::THROW_EXCEPTION ? "" : "IF EXISTS ";
	auto drop_sql = StringUtil::Format("DROP TABLE %s%s.%s", if_exists, schema_name, table_name);
	if (info.cascade) {
		drop_sql += " CASCADE";
	}

	auto &instance_state = GetDuckherderInstanceStateOrThrow(db_instance);
	const auto query_recorder_handle = instance_state.GetQueryRecorder()->RecordQueryStart(drop_sql);
	auto &client = duckherder_catalog.GetClient(context);
	auto result = client.ExecuteStatement(drop_sql, StatementType::DROP_STATEMENT);
	if (result->HasError()) {
		throw CatalogException("Failed to drop remote table on server: %s", result->GetError());
	}

	remote_indexes.RemoveTable(info.name);
	if (duckherder_catalog.IsRemoteTable(name, info.name)) {
		duckherder_catalog.UnregisterRemoteTable(info.name);
	}
}

void DuckherderSchemaCatalogEntry::DropRemoteView(ClientContext &context, const DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Dropping remote view: %s", info.name));

	auto schema_name = KeywordHelper::WriteQuoted(name, '"');
	auto view_name = KeywordHelper::WriteQuoted(info.name, '"');
	auto if_exists = info.if_not_found == OnEntryNotFound::THROW_EXCEPTION ? "" : "IF EXISTS ";
	auto drop_sql = StringUtil::Format("DROP VIEW %s%s.%s", if_exists, schema_name, view_name);
	if (info.cascade) {
		drop_sql += " CASCADE";
	}

	auto &client = duckherder_catalog.GetClient(context);
	auto result = client.ExecuteStatement(drop_sql, StatementType::DROP_STATEMENT);
	if (result->HasError()) {
		throw CatalogException("Failed to drop remote view on server: %s", result->GetError());
	}
}

void DuckherderSchemaCatalogEntry::DropRemoteType(ClientContext &context, const DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Dropping remote type: %s", info.name));

	auto schema_name = KeywordHelper::WriteQuoted(name, '"');
	auto type_name = KeywordHelper::WriteQuoted(info.name, '"');
	auto if_exists = info.if_not_found == OnEntryNotFound::THROW_EXCEPTION ? "" : "IF EXISTS ";
	auto drop_sql = StringUtil::Format("DROP TYPE %s%s.%s", if_exists, schema_name, type_name);
	if (info.cascade) {
		drop_sql += " CASCADE";
	}

	auto &client = duckherder_catalog.GetClient(context);
	auto result = client.ExecuteStatement(drop_sql, StatementType::DROP_STATEMENT);
	if (result->HasError()) {
		throw CatalogException("Failed to drop remote type on server: %s", result->GetError());
	}
}

void DuckherderSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("DuckherderSchemaCatalogEntry::DropEntry - type=%s name=%s",
	                                                 CatalogTypeToString(info.type), info.name));

	if (info.type == CatalogType::INDEX_ENTRY) {
		DropRemoteIndex(context, info);
	} else if (info.type == CatalogType::TABLE_ENTRY) {
		DropRemoteTable(context, info);
	} else if (info.type == CatalogType::VIEW_ENTRY) {
		DropRemoteView(context, info);
	} else if (info.type == CatalogType::TYPE_ENTRY) {
		DropRemoteType(context, info);
	}

	DuckSchemaEntry::DropEntry(context, info);

	// Remove from cache after successful drop.
	EntryLookupInfoKey key {
	    .type = info.type,
	    .name = info.name,
	};
	concurrency::lock_guard<concurrency::mutex> lck(mu);
	// Here we don't check erase result since we haven't implemented all catalog entry types.
	catalog_entries.erase(key);
}

void DuckherderSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderSchemaCatalogEntry::Alter");

	string alter_sql;
	bool alter_view = false;
	if (info.type == AlterType::ALTER_TABLE) {
		auto &table_info = info.Cast<AlterTableInfo>();

		// CREATE/DROP TABLE with a foreign key already updates both tables on the control node. DuckDB issues this
		// internal ALTER only to mirror the foreign-key metadata into the referenced table's local catalog entry.
		if (table_info.alter_table_type != AlterTableType::FOREIGN_KEY_CONSTRAINT) {
			auto schema_name = KeywordHelper::WriteQuoted(name, '"');
			auto table_name = KeywordHelper::WriteQuoted(info.name, '"');
			auto qualified_table_name = StringUtil::Format("%s.%s", schema_name, table_name);
			alter_sql = GenerateAlterTableSQL(table_info, qualified_table_name);
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing ALTER TABLE on remote server: %s", alter_sql));
		}
	} else if (info.type == AlterType::ALTER_VIEW) {
		alter_sql = info.ToString();
		alter_view = true;
	}

	auto &context = transaction.GetContext();
	AlterLocal(std::move(transaction), info);
	if (alter_sql.empty()) {
		return;
	}

	auto &client = duckherder_catalog.GetClient(context);
	auto result = alter_view
	                  ? client.ExecuteStatement(alter_sql, StatementType::ALTER_STATEMENT, duckherder_catalog.GetName())
	                  : client.ExecuteStatement(alter_sql, StatementType::ALTER_STATEMENT);
	if (result->HasError()) {
		throw CatalogException("Failed to alter %s on server: %s", alter_view ? "view" : "table", result->GetError());
	}
}

void DuckherderSchemaCatalogEntry::AlterLocal(CatalogTransaction transaction, AlterInfo &info) {
	string renamed_table;
	if (info.type == AlterType::ALTER_TABLE) {
		auto &table_info = info.Cast<AlterTableInfo>();
		EntryLookupInfoKey key {
		    .type = CatalogType::TABLE_ENTRY,
		    .name = info.name,
		};
		concurrency::lock_guard<concurrency::mutex> lck(mu);
		catalog_entries.erase(key);
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Cleared cache for table %s after ALTER", info.name));
		if (table_info.alter_table_type == AlterTableType::RENAME_TABLE) {
			renamed_table = table_info.Cast<RenameTableInfo>().new_table_name;
		}
	}

	DuckSchemaEntry::Alter(std::move(transaction), info);
	if (!renamed_table.empty() && duckherder_catalog.IsRemoteTable(name, info.name)) {
		duckherder_catalog.UnregisterRemoteTable(info.name);
	}
}

} // namespace duckdb
