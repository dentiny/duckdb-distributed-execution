#include "duckherder_catalog.hpp"

#include "distributed_client.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "logical_remote_create_index.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckherder_schema_catalog_entry.hpp"
#include "duckherder_transaction.hpp"

namespace duckdb {

namespace {

bool IsSupportedInsertSource(InsertStatement &insert) {
	if (insert.GetValuesList()) {
		return true;
	}
	if (!insert.select_statement || insert.select_statement->node->type != QueryNodeType::SELECT_NODE) {
		return false;
	}
	auto &select = insert.select_statement->node->Cast<SelectNode>();
	if (!select.from_table || select.from_table->type != TableReferenceType::TABLE_FUNCTION) {
		return false;
	}
	auto &table_function = select.from_table->Cast<TableFunctionRef>();
	if (!table_function.function || table_function.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &function = table_function.function->Cast<FunctionExpression>();
	return function.function_name == "range" || function.function_name == "generate_series";
}

unique_ptr<TableRef> CreateRemoteDMLRef(const string &catalog_name, const string &local_table_name, string sql,
                                        DatabaseModificationType modification) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(Value(catalog_name)));
	arguments.push_back(make_uniq<ConstantExpression>(Value(local_table_name)));
	arguments.push_back(make_uniq<ConstantExpression>(Value(std::move(sql))));
	if (modification.InsertData()) {
		arguments.push_back(make_uniq<ConstantExpression>(Value::BIGINT(DatabaseModificationType::INSERT_DATA)));
	} else {
		D_ASSERT(modification.DeleteData());
		arguments.push_back(make_uniq<ConstantExpression>(Value::BIGINT(DatabaseModificationType::DELETE_DATA)));
	}
	auto result = make_uniq<TableFunctionRef>();
	result->function = make_uniq<FunctionExpression>("duckherder_remote_query", std::move(arguments));
	return std::move(result);
}

} // namespace

DuckherderCatalog::DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p,
                                     string server_db_path_p)
    : DuckCatalog(db), duckdb_catalog(make_uniq<DuckCatalog>(db)), db_instance(db.GetDatabase()),
      server_host(std::move(server_host_p)), server_port(server_port_p), server_db_path(std::move(server_db_path_p)) {
}

DuckherderCatalog::~DuckherderCatalog() = default;

void DuckherderCatalog::Initialize(bool load_builtin) {
	duckdb_catalog->Initialize(load_builtin);
}

bool DuckherderCatalog::Supports(RemoteCapability capability) const {
	switch (capability) {
	case RemoteCapability::IS_REMOTE:
	case RemoteCapability::EXECUTE_STATEMENT:
		return true;
	default:
		return false;
	}
}

unique_ptr<TableRef> DuckherderCatalog::RemoteExecute(ClientContext &context,
                                                      unique_ptr<SQLStatement> statement) {
	if (!statement->named_param_map.empty()) {
		throw NotImplementedException(
		    "Duckherder remote INSERT and DELETE do not yet support prepared statement parameters");
	}
	DatabaseModificationType modification;
	string local_table_name;
	if (statement->type == StatementType::INSERT_STATEMENT) {
		auto &insert = statement->Cast<InsertStatement>();
		if (!insert.returning_list.empty() || insert.on_conflict_info || !insert.cte_map.map.empty() ||
		    !IsSupportedInsertSource(insert)) {
			throw NotImplementedException(
			    "Duckherder remote INSERT supports VALUES, range, and generate_series without parameters, CTEs, "
			    "ON CONFLICT, or RETURNING");
		}
		local_table_name = insert.table;
		if (!IsRemoteTable(local_table_name)) {
			throw NotImplementedException(
			    "Duckherder catalogs are remote-authoritative; INSERT into an unregistered table is unsupported");
		}
		auto remote_name = QualifiedName::Parse(GetRemoteTableConfig(local_table_name).remote_table_name);
		insert.catalog = remote_name.catalog;
		insert.schema = remote_name.schema;
		insert.table = remote_name.name;
		modification = DatabaseModificationType::INSERT_DATA;
	} else if (statement->type == StatementType::DELETE_STATEMENT) {
		auto &del = statement->Cast<DeleteStatement>();
		if (!del.returning_list.empty() || !del.using_clauses.empty() || !del.table ||
		    del.table->type != TableReferenceType::BASE_TABLE || !del.cte_map.map.empty()) {
			throw NotImplementedException("Duckherder remote DELETE does not support CTEs, USING, or RETURNING");
		}

		auto &target = del.table->Cast<BaseTableRef>();
		local_table_name = target.table_name;
		if (!IsRemoteTable(local_table_name)) {
			throw NotImplementedException(
			    "Duckherder catalogs are remote-authoritative; DELETE from an unregistered table is unsupported");
		}

		auto remote_name = QualifiedName::Parse(GetRemoteTableConfig(local_table_name).remote_table_name);
		if (target.alias.empty()) {
			// Preserve references such as "local_name.id" after replacing the
			// physical target with its control-node name.
			target.alias = local_table_name;
		}
		target.catalog_name = remote_name.catalog;
		target.schema_name = remote_name.schema;
		target.table_name = remote_name.name;
		modification = DatabaseModificationType::DELETE_DATA;
	} else {
		throw NotImplementedException("Duckherder v1.5.5 statement pushdown only supports INSERT and DELETE");
	}
	return CreateRemoteDMLRef(GetName(), local_table_name, statement->ToString(), modification);
}

optional_ptr<CatalogEntry> DuckherderCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::CreateSchema");
	return duckdb_catalog->CreateSchema(std::move(transaction), info);
}

optional_ptr<SchemaCatalogEntry> DuckherderCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	auto entry_lookup_str = schema_lookup.GetEntryName();
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("DuckherderCatalog::LookupSchema %s", entry_lookup_str));

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
		auto duckherder_schema_entry = make_uniq<DuckherderSchemaCatalogEntry>(*this, db_instance, schema_catalog_entry,
		                                                                       std::move(create_schema_info));
		iter = schema_catalog_entries.emplace(std::move(entry_lookup_str), std::move(duckherder_schema_entry)).first;
	}

	return iter->second.get();
}

void DuckherderCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::ScanSchemas");
	duckdb_catalog->ScanSchemas(context, std::move(callback));
}

PhysicalOperator &DuckherderCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanCreateTableAs");
	throw NotImplementedException("Duckherder catalogs are remote-authoritative; CREATE TABLE AS is unsupported");
}

PhysicalOperator &DuckherderCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanInsert");

	// Attempt insertion into remote table if registered.
	bool is_remote = IsRemoteTable(op.table.name);
	if (is_remote) {
		if (op.return_chunk) {
			throw NotImplementedException("Duckherder remote INSERT does not support RETURNING");
		}
		throw NotImplementedException("Unsupported remote INSERT; refusing physical fallback");
	}

	throw NotImplementedException(
	    "Duckherder catalogs are remote-authoritative; INSERT into an unregistered local table is unsupported");
}

PhysicalOperator &DuckherderCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanDelete");

	// Attempt deletion from remote table if registered.
	bool is_remote = IsRemoteTable(op.table.name);
	if (is_remote) {
		if (op.return_chunk) {
			throw NotImplementedException("Duckherder remote DELETE does not support RETURNING");
		}
		throw NotImplementedException("Unsupported remote DELETE; refusing physical fallback");
	}

	throw NotImplementedException(
	    "Duckherder catalogs are remote-authoritative; DELETE from an unregistered local table is unsupported");
}

PhysicalOperator &DuckherderCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanUpdate");
	if (IsRemoteTable(op.table.name)) {
		if (op.return_chunk) {
			throw NotImplementedException("Duckherder remote UPDATE does not support RETURNING");
		}
		throw NotImplementedException("Duckherder remote UPDATE is unsupported; refusing physical fallback");
	}
	throw NotImplementedException(
	    "Duckherder catalogs are remote-authoritative; UPDATE of an unregistered local table is unsupported");
}

unique_ptr<LogicalOperator> DuckherderCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                               TableCatalogEntry &table,
                                                               unique_ptr<LogicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::BindCreateIndex");

	// Attempt remote table if applicable.
	string table_name = table.name;
	if (IsRemoteTable(table_name)) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Bind CREATE INDEX on remote table %s", table_name));
		// For remote tables, we use a custom logical operator that doesn't require scanning the table locally.
		// The index will be created on the remote server via DuckherderSchemaCatalogEntry::CreateIndex.
		auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
		return make_uniq<LogicalRemoteCreateIndexOperator>(std::move(create_index_info), table.schema, table);
	}

	// Fallback to local tables.
	return duckdb_catalog->BindCreateIndex(binder, stmt, table, std::move(plan));
}

unique_ptr<LogicalOperator> DuckherderCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                                 unique_ptr<LogicalOperator> plan,
                                                                 unique_ptr<CreateIndexInfo> create_info,
                                                                 unique_ptr<AlterTableInfo> alter_info) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::BindAlterAddIndex");
	return duckdb_catalog->BindAlterAddIndex(binder, table_entry, std::move(plan), std::move(create_info),
	                                         std::move(alter_info));
}

DatabaseSize DuckherderCatalog::GetDatabaseSize(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::GetDatabaseSize");
	return duckdb_catalog->GetDatabaseSize(context);
}

vector<MetadataBlockInfo> DuckherderCatalog::GetMetadataInfo(ClientContext &context) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::GetMetadataInfo");
	return duckdb_catalog->GetMetadataInfo(context);
}

bool DuckherderCatalog::InMemory() {
	return duckdb_catalog->InMemory();
}

string DuckherderCatalog::GetDBPath() {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::GetDBPath", duckdb_catalog->GetDBPath());
	return duckdb_catalog->GetDBPath();
}

bool DuckherderCatalog::IsEncrypted() const {
	return duckdb_catalog->IsEncrypted();
}

string DuckherderCatalog::GetEncryptionCipher() const {
	return duckdb_catalog->GetEncryptionCipher();
}

optional_idx DuckherderCatalog::GetCatalogVersion(ClientContext &context) {
	return duckdb_catalog->GetCatalogVersion(context);
}

optional_ptr<DependencyManager> DuckherderCatalog::GetDependencyManager() {
	return duckdb_catalog->GetDependencyManager();
}

void DuckherderCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO(hjiang): Implement drop feature.
	throw NotImplementedException("DropSchema not implemented");
}

void DuckherderCatalog::RegisterRemoteTable(const string &table_name, const string &server_url,
                                            const string &remote_table_name) {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	auto remote_table_config = RemoteTableConfig(server_url, remote_table_name);
	const bool succ = remote_tables.emplace(table_name, std::move(remote_table_config)).second;
	if (!succ) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to register table %s because it's already registered!", table_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Registered remote table %s -> %s:%s", table_name, server_url,
	                                                 remote_table_name));
}

void DuckherderCatalog::UnregisterRemoteTable(const string &table_name) {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	const size_t count = remote_tables.erase(table_name);
	if (count != 1) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to unregister table %s because it hasn't been registered!", table_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Unregistered remote table %s", table_name));
}

bool DuckherderCatalog::IsRemoteTable(const string &table_name) const {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	auto it = remote_tables.find(table_name);
	bool found = it != remote_tables.end() && it->second.is_distributed;
	return found;
}

bool DuckherderCatalog::IsRegisteredRemoteTarget(const string &local_table_name, const BaseTableRef &target) const {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	auto entry = remote_tables.find(local_table_name);
	if (entry == remote_tables.end() || !entry->second.is_distributed) {
		return false;
	}
	auto remote_name = QualifiedName::Parse(entry->second.remote_table_name);
	return remote_name.catalog == target.catalog_name && remote_name.schema == target.schema_name &&
	       remote_name.name == target.table_name;
}

RemoteTableConfig DuckherderCatalog::GetRemoteTableConfig(const string &table_name) const {
	std::lock_guard<std::mutex> lck(remote_tables_mu);
	auto it = remote_tables.find(table_name);
	if (it != remote_tables.end()) {
		return it->second;
	}
	// Fallbacks to default, which is not distributed table.
	return RemoteTableConfig();
}

void DuckherderCatalog::RegisterRemoteIndex(const string &index_name) {
	std::lock_guard<std::mutex> lck(remote_indexes_mu);
	const bool succ = remote_indexes.insert(index_name).second;
	if (!succ) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to register index %s because it's already registered!", index_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Registered remote index %s", index_name));
}

void DuckherderCatalog::UnregisterRemoteIndex(const string &index_name) {
	std::lock_guard<std::mutex> lck(remote_indexes_mu);
	const size_t count = remote_indexes.erase(index_name);
	if (count != 1) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to unregister index %s because it hasn't been registered!", index_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Unregistered remote index %s", index_name));
}

bool DuckherderCatalog::IsRemoteIndex(const string &index_name) const {
	std::lock_guard<std::mutex> lck(remote_indexes_mu);
	return remote_indexes.find(index_name) != remote_indexes.end();
}

string DuckherderCatalog::GetServerUrl() const {
	return StringUtil::Format("grpc://%s:%d", server_host, server_port);
}

DistributedClient &DuckherderCatalog::GetClient() {
	std::lock_guard<std::mutex> lock(client_mu);
	if (!distributed_client) {
		distributed_client = make_uniq<DistributedClient>(GetServerUrl());
	}
	return *distributed_client;
}

} // namespace duckdb
