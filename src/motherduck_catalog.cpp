#include "duckdb/storage/database_size.hpp"
#include "motherduck_catalog.hpp"
#include "motherduck_transaction.hpp"

namespace duckdb {

MotherduckCatalog::MotherduckCatalog(AttachedDatabase &db, string uri, string database) : Catalog(db), uri(uri) {
}

MotherduckCatalog::~MotherduckCatalog() = default;

void MotherduckCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> MotherduckCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("CreateSchema not implemented");
}

optional_ptr<SchemaCatalogEntry> MotherduckCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	return transaction.transaction->Cast<MotherduckTransaction>().GetOrCreateSchema(schema_lookup.GetEntryName());
}

void MotherduckCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
}

PhysicalOperator &MotherduckCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("PlanCreateTableAs not implemented");
}

PhysicalOperator &MotherduckCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanInsert not implemented");
}

PhysicalOperator &MotherduckCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented");
}

PhysicalOperator &MotherduckCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented");
}

unique_ptr<LogicalOperator> MotherduckCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                               TableCatalogEntry &table,
                                                               unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented");
}

DatabaseSize MotherduckCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented");
}

bool MotherduckCatalog::InMemory() {
	return false;
}

string MotherduckCatalog::GetDBPath() {
	return uri;
}

void MotherduckCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropSchema not implemented");
}

} // namespace duckdb
