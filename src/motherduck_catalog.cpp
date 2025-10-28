#include "motherduck_catalog.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "motherduck_schema_catalog_entry.hpp"
#include "motherduck_transaction.hpp"
#include "physical_distributed_insert.hpp"

namespace duckdb {

MotherduckCatalog::MotherduckCatalog(AttachedDatabase &db)
    : DuckCatalog(db), duckdb_catalog(make_uniq<DuckCatalog>(db)), db_instance(db.GetDatabase()) {
}

MotherduckCatalog::~MotherduckCatalog() = default;

void MotherduckCatalog::Initialize(bool load_builtin) {
	duckdb_catalog->Initialize(load_builtin);
}

optional_ptr<CatalogEntry> MotherduckCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::CreateSchema");
	return duckdb_catalog->CreateSchema(std::move(transaction), info);
}

optional_ptr<SchemaCatalogEntry> MotherduckCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	auto entry_lookup_str = schema_lookup.GetEntryName();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("MotherduckCatalog::LookupSchema %s", entry_lookup_str));

	std::lock_guard<std::mutex> lck(mu);
	auto iter = schema_catalog_entries.find(entry_lookup_str);
	if (iter == schema_catalog_entries.end()) {
		auto catalog_entry = duckdb_catalog->LookupSchema(std::move(transaction), schema_lookup, if_not_found);
		if (!catalog_entry) {
			return catalog_entry;
		}

		auto create_schema_info = make_uniq<CreateSchemaInfo>();
		create_schema_info->schema = catalog_entry->name;
		create_schema_info->comment = catalog_entry->comment;
		create_schema_info->tags = catalog_entry->tags;

		auto *schema_catalog_entry = dynamic_cast<SchemaCatalogEntry *>(catalog_entry.get());
		D_ASSERT(schema_catalog_entry != nullptr);
		auto motherduck_schema_entry = make_uniq<MotherduckSchemaCatalogEntry>(*this, db_instance, schema_catalog_entry,
		                                                                       std::move(create_schema_info));
		iter = schema_catalog_entries.emplace(std::move(entry_lookup_str), std::move(motherduck_schema_entry)).first;
	}

	return iter->second.get();
}

void MotherduckCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::ScanSchemas");
	duckdb_catalog->ScanSchemas(context, std::move(callback));
}

PhysicalOperator &MotherduckCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::PlanCreateTableAs");
	return duckdb_catalog->PlanCreateTableAs(context, planner, op, plan);
}

PhysicalOperator &MotherduckCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::PlanInsert");

	// Check if this is a distributed table
	bool is_remote = IsRemoteTable(op.table.name);
	if (is_remote) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Execute remote insertion to table %s", op.table.name));

		D_ASSERT(plan);
		auto &distributed_insert = planner.Make<PhysicalDistributedInsert>(op.table, *plan, op.estimated_cardinality);
		// Note: children are added in the PhysicalDistributedInsert, don't add here.
		return distributed_insert;
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Execute local insertion to table %s", op.table.name));
	return duckdb_catalog->PlanInsert(context, planner, op, plan);
}

PhysicalOperator &MotherduckCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::PlanDelete");
	return duckdb_catalog->PlanDelete(context, planner, op, plan);
}

PhysicalOperator &MotherduckCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::PlanUpdate");
	return duckdb_catalog->PlanUpdate(context, planner, op, plan);
}

unique_ptr<LogicalOperator> MotherduckCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                               TableCatalogEntry &table,
                                                               unique_ptr<LogicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::BindCreateIndex");
	return duckdb_catalog->BindCreateIndex(binder, stmt, table, std::move(plan));
}

unique_ptr<LogicalOperator> MotherduckCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                                 unique_ptr<LogicalOperator> plan,
                                                                 unique_ptr<CreateIndexInfo> create_info,
                                                                 unique_ptr<AlterTableInfo> alter_info) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::BindAlterAddIndex");
	return duckdb_catalog->BindAlterAddIndex(binder, table_entry, std::move(plan), std::move(create_info),
	                                         std::move(alter_info));
}

DatabaseSize MotherduckCatalog::GetDatabaseSize(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::GetDatabaseSize");
	return duckdb_catalog->GetDatabaseSize(context);
}

vector<MetadataBlockInfo> MotherduckCatalog::GetMetadataInfo(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::GetMetadataInfo");
	return duckdb_catalog->GetMetadataInfo(context);
}

bool MotherduckCatalog::InMemory() {
	return duckdb_catalog->InMemory();
}

string MotherduckCatalog::GetDBPath() {
	DUCKDB_LOG_DEBUG(db_instance, "MotherduckCatalog::GetDBPath", duckdb_catalog->GetDBPath());
	return duckdb_catalog->GetDBPath();
}

bool MotherduckCatalog::IsEncrypted() const {
	return duckdb_catalog->IsEncrypted();
}

string MotherduckCatalog::GetEncryptionCipher() const {
	return duckdb_catalog->GetEncryptionCipher();
}

optional_idx MotherduckCatalog::GetCatalogVersion(ClientContext &context) {
	return duckdb_catalog->GetCatalogVersion(context);
}

optional_ptr<DependencyManager> MotherduckCatalog::GetDependencyManager() {
	return duckdb_catalog->GetDependencyManager();
}

void MotherduckCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO(hjiang): Implement drop feature.
	throw NotImplementedException("DropSchema not implemented");
}

void MotherduckCatalog::RegisterRemoteTable(const string &table_name, const string &server_url,
                                            const string &remote_table_name) {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	remote_tables[table_name] = RemoteTableConfig(server_url, remote_table_name);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Registered remote table %s -> %s:%s", table_name, server_url,
	                                                 remote_table_name));
}

void MotherduckCatalog::UnregisterRemoteTable(const string &table_name) {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	remote_tables.erase(table_name);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Unregistered remote table %s", table_name));
}

bool MotherduckCatalog::IsRemoteTable(const string &table_name) const {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	auto it = remote_tables.find(table_name);
	bool found = it != remote_tables.end() && it->second.is_distributed;
	return found;
}

RemoteTableConfig MotherduckCatalog::GetRemoteTableConfig(const string &table_name) const {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	auto it = remote_tables.find(table_name);
	if (it != remote_tables.end()) {
		return it->second;
	}
	// Fallbacks to default, which is not distributed table.
	return RemoteTableConfig();
}

} // namespace duckdb
