#include "duckherder_catalog.hpp"

#include "client/duckherder_catalog_loader.hpp"
#include "client/execution/distributed_client.hpp"
#include "client/execution/remote_create_table_as.hpp"
#include "client/execution/remote_dml.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "client/execution/logical_remote_alter_table.hpp"
#include "client/execution/logical_remote_create_index.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckherder_schema_catalog_entry.hpp"
#include "duckherder_transaction.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

DuckherderCatalog::DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p,
                                     distributed::ClientRole role_p, connection_t attach_connection_id_p,
                                     distributed::StorageConfig storage_config_p)
    : DuckCatalog(db), db_instance(db.GetDatabase()), server_host(std::move(server_host_p)), server_port(server_port_p),
      client_sessions(GetServerUrl(), role_p, db_instance, std::move(storage_config_p), attach_connection_id_p) {
}

DuckherderCatalog::~DuckherderCatalog() {
	client_sessions.Close();
}

void DuckherderCatalog::OnDetach(ClientContext &context) {
	client_sessions.Close();
	client_sessions.RemoveState(context);
}

void DuckherderCatalog::FinalizeLoad(optional_ptr<ClientContext> context) {
	DuckCatalog::FinalizeLoad(context);
	if (context) {
		DuckherderCatalogLoader(*this, *context).Load();
	}
}

optional_idx DuckherderCatalog::GetEstimatedCardinality(const string &schema_name, const string &table_name) const {
	concurrency::lock_guard<concurrency::mutex> lck(table_cardinalities_mu);
	auto entry = table_cardinalities.find(QualifiedRemoteName(schema_name, table_name));
	if (entry == table_cardinalities.end()) {
		return optional_idx();
	}
	return entry->second;
}

void DuckherderCatalog::SetEstimatedCardinality(const string &schema_name, const string &table_name,
                                                idx_t cardinality) {
	concurrency::lock_guard<concurrency::mutex> lck(table_cardinalities_mu);
	table_cardinalities[QualifiedRemoteName(schema_name, table_name)] = cardinality;
}

optional_ptr<CatalogEntry> DuckherderCatalog::CreateSchemaLocal(CatalogTransaction transaction,
                                                                CreateSchemaInfo &info) {
	LogicalDependencyList dependencies;
	auto entry = unique_ptr<DuckherderSchemaCatalogEntry>(new DuckherderSchemaCatalogEntry(*this, db_instance, info));
	auto result = entry.get();
	if (GetSchemaCatalogSet().CreateEntry(transaction, info.schema, std::move(entry), dependencies)) {
		return result;
	}

	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		return nullptr;
	}
	if (info.on_conflict != OnCreateConflict::ERROR_ON_CONFLICT &&
	    info.on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw InternalException("Unsupported OnCreateConflict for Duckherder schema");
	}
	if (!GetSchemaCatalogSet().DropEntry(transaction, info.schema, true)) {
		throw InternalException("Failed to refresh local schema cache entry %s", info.schema);
	}
	entry = unique_ptr<DuckherderSchemaCatalogEntry>(new DuckherderSchemaCatalogEntry(*this, db_instance, info));
	result = entry.get();
	if (!GetSchemaCatalogSet().CreateEntry(transaction, info.schema, std::move(entry), dependencies)) {
		throw InternalException("Failed to create refreshed local schema cache entry %s", info.schema);
	}
	return result;
}

optional_ptr<CatalogEntry> DuckherderCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::CreateSchema");
	if (info.internal) {
		D_ASSERT(info.schema == DEFAULT_SCHEMA);
		return CreateSchemaLocal(std::move(transaction), info);
	}
	if (DefaultSchemaGenerator::IsDefaultSchema(info.schema)) {
		return DuckCatalog::CreateSchema(std::move(transaction), info);
	}
	if (!transaction.HasContext()) {
		throw InternalException("Cannot create a remote Duckherder schema without a client context");
	}
	auto result = GetClient(transaction.GetContext())
	                  .ExecuteStatement(info.ToString(), StatementType::CREATE_STATEMENT, GetName());
	if (result->HasError()) {
		throw CatalogException("Failed to create schema on server: %s", result->GetError());
	}
	return CreateSchemaLocal(std::move(transaction), info);
}

PhysicalOperator &DuckherderCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanCreateTableAs");
	auto sql = GetRemoteStatementSQL(context);
	auto &schema = op.schema.Cast<DuckherderSchemaCatalogEntry>();
	return planner.Make<PhysicalRemoteCreateTableAs>(op, *this, schema, std::move(op.info), std::move(sql));
}

PhysicalOperator &DuckherderCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanInsert");

	auto sql = GetRemoteStatementSQL(context);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push INSERT to control node: %s", sql));
	return planner.Make<PhysicalRemoteDML>(PhysicalOperatorType::INSERT, op.types, op.table, std::move(sql),
	                                       op.estimated_cardinality);
}

PhysicalOperator &DuckherderCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanDelete");

	auto sql = GetRemoteStatementSQL(context);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push DELETE to control node: %s", sql));
	return planner.Make<PhysicalRemoteDML>(PhysicalOperatorType::DELETE_OPERATOR, op.types, op.table, std::move(sql),
	                                       op.estimated_cardinality);
}

PhysicalOperator &DuckherderCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanUpdate");
	auto sql = GetRemoteStatementSQL(context);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push UPDATE to control node: %s", sql));
	return planner.Make<PhysicalRemoteDML>(PhysicalOperatorType::UPDATE, op.types, op.table, std::move(sql),
	                                       op.estimated_cardinality);
}

PhysicalOperator &DuckherderCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalMergeInto &op, PhysicalOperator &plan) {
	auto sql = GetRemoteStatementSQL(context);
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		return DuckCatalog::PlanMergeInto(context, planner, op, plan);
	}
	auto statement_type = parser.statements[0]->type;
	if (statement_type != StatementType::INSERT_STATEMENT && statement_type != StatementType::MERGE_INTO_STATEMENT) {
		return DuckCatalog::PlanMergeInto(context, planner, op, plan);
	}
	auto operator_type = statement_type == StatementType::INSERT_STATEMENT ? PhysicalOperatorType::INSERT
	                                                                       : PhysicalOperatorType::MERGE_INTO;

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push MERGE to control node: %s", sql));
	return planner.Make<PhysicalRemoteDML>(operator_type, op.types, op.table, std::move(sql), op.estimated_cardinality);
}

unique_ptr<LogicalOperator> DuckherderCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                               TableCatalogEntry &table,
                                                               unique_ptr<LogicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::BindCreateIndex");

	string table_name = table.name;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Bind CREATE INDEX on remote table %s", table_name));
	auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
	return make_uniq<LogicalRemoteCreateIndexOperator>(std::move(create_index_info), table.schema, table);
}

unique_ptr<LogicalOperator> DuckherderCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                                 unique_ptr<LogicalOperator> plan,
                                                                 unique_ptr<CreateIndexInfo> create_info,
                                                                 unique_ptr<AlterTableInfo> alter_info) {
	if (table_entry.internal) {
		return DuckCatalog::BindAlterAddIndex(binder, table_entry, std::move(plan), std::move(create_info),
		                                      std::move(alter_info));
	}
	return make_uniq<LogicalRemoteAlterTableOperator>(std::move(alter_info), table_entry.schema, table_entry);
}

void DuckherderCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto transaction = GetCatalogTransaction(context);
	auto schema = GetSchemaCatalogSet().GetEntry(transaction, info.name);
	if (schema && schema->internal) {
		throw CatalogException("Cannot drop internal schema \"%s\"", info.name);
	}
	auto result = GetClient(context).ExecuteStatement(info.ToString(), StatementType::DROP_STATEMENT, GetName());
	if (result->HasError()) {
		throw CatalogException("Failed to drop schema on server: %s", result->GetError());
	}
	// The remote catalog is authoritative. After it accepts the DROP, remove any stale local children as well.
	GetSchemaCatalogSet().DropEntry(transaction, info.name, true);
}

void DuckherderCatalog::RegisterRemoteTable(const string &table_name, const string &server_url,
                                            const string &remote_table_name) {
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
	auto remote_table_config = RemoteTableConfig(server_url, remote_table_name);
	const bool succ = remote_tables.emplace(table_name, std::move(remote_table_config)).second;
	if (!succ) {
		throw InvalidInputException("Failed to register table %s because it's already registered!", table_name);
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Registered remote table %s -> %s:%s", table_name, server_url,
	                                                 remote_table_name));
}

void DuckherderCatalog::UnregisterRemoteTable(const string &table_name) {
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
	if (remote_tables.erase(table_name) != 1) {
		throw InvalidInputException("Failed to unregister table %s because it hasn't been registered!", table_name);
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Unregistered remote table %s", table_name));
}

bool DuckherderCatalog::IsRemoteTable(const string &schema_name, const string &table_name) const {
	if (!StringUtil::CIEquals(schema_name, DEFAULT_SCHEMA)) {
		return false;
	}
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
	return remote_tables.find(table_name) != remote_tables.end();
}

RemoteTableConfig DuckherderCatalog::GetRemoteTableConfig(const string &schema_name, const string &table_name) const {
	if (StringUtil::CIEquals(schema_name, DEFAULT_SCHEMA)) {
		concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
		auto table = remote_tables.find(table_name);
		if (table != remote_tables.end()) {
			return table->second;
		}
	}
	return RemoteTableConfig(StringUtil::Format("grpc://%s:%d", server_host, server_port),
	                         QualifiedRemoteName(schema_name, table_name));
}

string DuckherderCatalog::GetServerUrl() const {
	return StringUtil::Format("grpc://%s:%d", server_host, server_port);
}

DistributedClient &DuckherderCatalog::GetClient(ClientContext &context) {
	return client_sessions.GetClient(context);
}

} // namespace duckdb
