#include "duckling_catalog.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckling_schema_catalog_entry.hpp"
#include <iostream>

namespace duckdb {

DucklingCatalog::DucklingCatalog(AttachedDatabase &db)
    : DuckCatalog(db), duckdb_catalog(make_uniq<DuckCatalog>(db)), db_instance(db.GetDatabase()) {
	std::cerr << "[DUCKLING CATALOG] DucklingCatalog initialized for server-side storage" << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog initialized for server-side storage");
}

DucklingCatalog::~DucklingCatalog() = default;

void DucklingCatalog::Initialize(bool load_builtin) {
	duckdb_catalog->Initialize(load_builtin);
}

optional_ptr<CatalogEntry> DucklingCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("DucklingCatalog::CreateSchema: %s", info.schema));
	return duckdb_catalog->CreateSchema(std::move(transaction), info);
}

optional_ptr<SchemaCatalogEntry> DucklingCatalog::LookupSchema(CatalogTransaction transaction,
                                                               const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	auto entry_lookup_str = schema_lookup.GetEntryName();
	std::cerr << "[DUCKLING CATALOG] LookupSchema: " << entry_lookup_str << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("DucklingCatalog::LookupSchema %s", entry_lookup_str));
	
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
		auto duckling_schema_entry = make_uniq<DucklingSchemaCatalogEntry>(*this, db_instance, schema_catalog_entry,
		                                                                   std::move(create_schema_info));
		iter = schema_catalog_entries.emplace(std::move(entry_lookup_str), std::move(duckling_schema_entry)).first;
	}

	return iter->second.get();
}

void DucklingCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::ScanSchemas");
	duckdb_catalog->ScanSchemas(context, std::move(callback));
}

PhysicalOperator &DucklingCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {
	std::cerr << "[DUCKLING CATALOG] PlanCreateTableAs called" << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::PlanCreateTableAs");
	return duckdb_catalog->PlanCreateTableAs(context, planner, op, plan);
}

PhysicalOperator &DucklingCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {
	std::cerr << "[DUCKLING CATALOG] PlanInsert called" << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::PlanInsert");

	// For now, just pass through to DuckCatalog
	// In the future, this could support fleet distribution
	return duckdb_catalog->PlanInsert(context, planner, op, plan);
}

PhysicalOperator &DucklingCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                              PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::PlanDelete");
	return duckdb_catalog->PlanDelete(context, planner, op, plan);
}

PhysicalOperator &DucklingCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                              PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::PlanUpdate");
	return duckdb_catalog->PlanUpdate(context, planner, op, plan);
}

unique_ptr<LogicalOperator> DucklingCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	std::cerr << "[DUCKLING CATALOG] BindCreateIndex called on table: " << table.name << std::endl;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("DucklingCatalog::BindCreateIndex on table: %s", table.name));
	return duckdb_catalog->BindCreateIndex(binder, stmt, table, std::move(plan));
}

unique_ptr<LogicalOperator> DucklingCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                               unique_ptr<LogicalOperator> plan,
                                                               unique_ptr<CreateIndexInfo> create_info,
                                                               unique_ptr<AlterTableInfo> alter_info) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::BindAlterAddIndex");
	return duckdb_catalog->BindAlterAddIndex(binder, table_entry, std::move(plan), std::move(create_info),
	                                         std::move(alter_info));
}

DatabaseSize DucklingCatalog::GetDatabaseSize(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::GetDatabaseSize");
	return duckdb_catalog->GetDatabaseSize(context);
}

vector<MetadataBlockInfo> DucklingCatalog::GetMetadataInfo(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::GetMetadataInfo");
	return duckdb_catalog->GetMetadataInfo(context);
}

bool DucklingCatalog::InMemory() {
	return duckdb_catalog->InMemory();
}

string DucklingCatalog::GetDBPath() {
	DUCKDB_LOG_DEBUG(db_instance, "DucklingCatalog::GetDBPath");
	return duckdb_catalog->GetDBPath();
}

bool DucklingCatalog::IsEncrypted() const {
	return duckdb_catalog->IsEncrypted();
}

string DucklingCatalog::GetEncryptionCipher() const {
	return duckdb_catalog->GetEncryptionCipher();
}

optional_idx DucklingCatalog::GetCatalogVersion(ClientContext &context) {
	return duckdb_catalog->GetCatalogVersion(context);
}

optional_ptr<DependencyManager> DucklingCatalog::GetDependencyManager() {
	return duckdb_catalog->GetDependencyManager();
}

void DucklingCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// DropSchema is a private method in the base Catalog class
	// For now, throw NotImplementedException since this is a no-op catalog wrapper
	// In the future, this could be implemented to handle schema drops with fleet management
	throw NotImplementedException("DucklingCatalog::DropSchema not implemented");
}

} // namespace duckdb
