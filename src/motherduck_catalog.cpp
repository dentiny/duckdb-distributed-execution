#include "motherduck_catalog.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/database_size.hpp"
#include "motherduck_schema.hpp"
#include "motherduck_transaction.hpp"

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
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("MotherduckCatalog::LookupSchema %s", schema_lookup.GetEntryName()));
	auto catalog_entry = duckdb_catalog->LookupSchema(std::move(transaction), schema_lookup, if_not_found);
	if (catalog_entry) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("Get schema catalog entry %s", catalog_entry->GetInfo()->ToString()));
	}
	return catalog_entry;
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
	throw NotImplementedException("DropSchema not implemented");
}

} // namespace duckdb
