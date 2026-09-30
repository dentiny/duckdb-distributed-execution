#include "duckherder_catalog.hpp"

#include "client/duckherder_connection_state.hpp"
#include "client/execution/distributed_client.hpp"
#include "client/execution/remote_dml.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
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

namespace {

unique_ptr<CreateInfo> ParseCreateInfo(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw CatalogException("Expected a single CREATE statement in remote catalog metadata: %s", sql);
	}
	return std::move(parser.statements[0]->Cast<CreateStatement>().info);
}

template <class FUNC>
void ForEachRow(DistributedClient &client, const string &sql, const vector<LogicalType> &types, FUNC &&callback) {
	auto result = client.ScanTable(sql, NO_QUERY_LIMIT, NO_QUERY_OFFSET, &types);
	if (result->HasError()) {
		throw IOException("Failed to load remote catalog metadata: %s", result->GetError());
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			callback(*chunk, row_idx);
		}
	}
}

string QuotedIdentifier(const string &name) {
	return KeywordHelper::WriteQuoted(name, '"');
}

string QualifiedRemoteName(const string &schema_name, const string &entry_name) {
	auto quoted_schema_name = QuotedIdentifier(schema_name);
	auto quoted_entry_name = QuotedIdentifier(entry_name);
	return StringUtil::Format("%s.%s", quoted_schema_name, quoted_entry_name);
}

} // namespace

namespace {

struct RemoteCreateTableAsSourceState : public GlobalSourceState {
	bool executed = false;
	unique_ptr<QueryResult> result;
};

} // namespace

class PhysicalRemoteCreateTableAs : public PhysicalOperator {
public:
	PhysicalRemoteCreateTableAs(PhysicalPlan &physical_plan, LogicalCreateTable &op, DuckherderCatalog &catalog_p,
	                            DuckherderSchemaCatalogEntry &schema_p, unique_ptr<BoundCreateTableInfo> info_p,
	                            string sql_p)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_TABLE, op.types, op.estimated_cardinality),
	      catalog(catalog_p), schema(schema_p), info(std::move(info_p)), sql(std::move(sql_p)) {
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		return make_uniq<RemoteCreateTableAsSourceState>();
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &state = input.global_state.Cast<RemoteCreateTableAsSourceState>();
		if (!state.executed) {
			state.executed = true;
			state.result = catalog.GetClient(context.client)
			                   .ExecuteStatement(sql, StatementType::CREATE_STATEMENT, catalog.GetName(), &types);
			if (state.result->HasError()) {
				throw CatalogException("Failed to execute CREATE TABLE AS on server: %s", state.result->GetError());
			}
			schema.CreateTableLocal(catalog.GetCatalogTransaction(context.client), *info);
		}

		auto result_chunk = state.result->Fetch();
		if (!result_chunk || result_chunk->size() == 0) {
			return SourceResultType::FINISHED;
		}
		chunk.Move(*result_chunk);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	bool IsSource() const override {
		return true;
	}

private:
	DuckherderCatalog &catalog;
	DuckherderSchemaCatalogEntry &schema;
	unique_ptr<BoundCreateTableInfo> info;
	string sql;
};

DuckherderCatalog::DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p,
                                     distributed::ClientRole role_p, connection_t attach_connection_id_p)
    : DuckCatalog(db), db_instance(db.GetDatabase()), server_host(std::move(server_host_p)), server_port(server_port_p),
      role(role_p), attach_connection_id(attach_connection_id_p),
      client_state_key(StringUtil::Format("duckherder_client_%s", UUID::ToString(UUID::GenerateRandomUUID()))) {
	attach_client = make_uniq<DistributedClient>(GetServerUrl(), role, db_instance);
}

DuckherderCatalog::~DuckherderCatalog() {
	CloseClients();
}

void DuckherderCatalog::OnDetach(ClientContext &context) {
	CloseClients();
	context.registered_state->Remove(client_state_key);
}

void DuckherderCatalog::FinalizeLoad(optional_ptr<ClientContext> context) {
	DuckCatalog::FinalizeLoad(context);
	if (context) {
		LoadRemoteCatalog(*context);
	}
}

void DuckherderCatalog::LoadRemoteCatalog(ClientContext &context) {
	auto &client = GetClient(context);
	auto transaction = CatalogTransaction::GetSystemTransaction(db_instance);

	// Fetch one ordered snapshot so concurrent remote DDL cannot leave a partially discovered catalog.
	ForEachRow(client,
	           "SELECT 0 AS entry_order, schema_name, NULL::VARCHAR AS entry_name, NULL::VARCHAR AS sql, "
	           "NULL::VARCHAR[] AS labels FROM duckdb_schemas() "
	           "WHERE database_name = current_database() AND schema_name <> 'main' AND NOT internal "
	           "UNION ALL "
	           "SELECT 1, schema_name, type_name, NULL::VARCHAR, labels FROM duckdb_types() "
	           "WHERE database_name = current_database() AND NOT internal AND labels IS NOT NULL "
	           "UNION ALL "
	           "SELECT 2, schema_name, table_name, sql, NULL::VARCHAR[] FROM duckdb_tables() "
	           "WHERE database_name = current_database() AND NOT internal "
	           "ORDER BY entry_order, schema_name, entry_name",
	           {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	            LogicalType::LIST(LogicalType::VARCHAR)},
	           [&](DataChunk &chunk, idx_t row_idx) {
		           auto entry_order = chunk.GetValue(0, row_idx).GetValue<int32_t>();
		           auto schema_name = chunk.GetValue(1, row_idx).GetValue<string>();
		           if (entry_order == 0) {
			           CreateSchemaInfo info;
			           info.schema = schema_name;
			           info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
			           CreateSchemaLocal(transaction, info);
			           return;
		           }

		           auto entry_name = chunk.GetValue(2, row_idx).GetValue<string>();
		           auto &schema = GetSchema(transaction, schema_name).Cast<DuckherderSchemaCatalogEntry>();
		           if (entry_order == 1) {
			           vector<string> labels;
			           auto labels_value = chunk.GetValue(4, row_idx);
			           for (auto &label : ListValue::GetChildren(labels_value)) {
				           labels.push_back(label.ToSQLString());
			           }
			           auto sql = StringUtil::Format("CREATE TYPE %s AS ENUM (%s)",
			                                         QualifiedRemoteName(schema_name, entry_name),
			                                         StringUtil::Join(labels, ", "));
			           auto info = ParseCreateInfo(sql);
			           auto &type_info = info->Cast<CreateTypeInfo>();
			           type_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
			           schema.CreateTypeLocal(transaction, type_info);
			           return;
		           }

		           auto info = unique_ptr_cast<CreateInfo, CreateTableInfo>(
		               ParseCreateInfo(chunk.GetValue(3, row_idx).GetValue<string>()));
		           info->schema = schema_name;
		           info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		           auto binder = Binder::CreateBinder(context);
		           auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);
		           schema.CreateTableLocal(transaction, *bound_info);
	           });
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
	if (statement_type != StatementType::INSERT_STATEMENT &&
	    statement_type != StatementType::MERGE_INTO_STATEMENT) {
		return DuckCatalog::PlanMergeInto(context, planner, op, plan);
	}
	auto operator_type = statement_type == StatementType::INSERT_STATEMENT ? PhysicalOperatorType::INSERT
	                                                                      : PhysicalOperatorType::MERGE_INTO;

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push MERGE to control node: %s", sql));
	return planner.Make<PhysicalRemoteDML>(operator_type, op.types, op.table, std::move(sql),
	                                       op.estimated_cardinality);
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
	concurrency::lock_guard<concurrency::mutex> lock(client_states_mu);
	if (detached) {
		throw InvalidInputException("Duckherder attachment is detached");
	}
	if (role == distributed::CLIENT_ROLE_READ_WRITE) {
		EnsureWriteOwner(context);
	}
	return GetOrCreateClientState(context)->GetClient();
}

void DuckherderCatalog::EnsureWriteOwner(ClientContext &context) {
	if (context.GetConnectionId() == attach_connection_id) {
		return;
	}
	auto owner_state = client_states.find(attach_connection_id);
	if (owner_state != client_states.end() && !owner_state->second.expired()) {
		throw InvalidInputException(
		    "A read-write Duckherder attachment can only be used by the DuckDB connection that attached it; "
		    "attach a separate Duckherder database with READ_ONLY access from this connection");
	}
	attach_connection_id = context.GetConnectionId();
}

shared_ptr<DuckherderConnectionState> DuckherderCatalog::GetOrCreateClientState(ClientContext &context) {
	shared_ptr<DuckherderConnectionState> state;
	if (context.GetConnectionId() == attach_connection_id && attach_client) {
		state = context.registered_state->GetOrCreate<DuckherderConnectionState>(client_state_key,
		                                                                         std::move(attach_client));
	} else {
		state = context.registered_state->GetOrCreate<DuckherderConnectionState>(client_state_key, GetServerUrl(), role,
		                                                                         db_instance);
	}
	PruneExpiredClientStates();
	client_states[context.GetConnectionId()] = state;
	return state;
}

void DuckherderCatalog::PruneExpiredClientStates() {
	for (auto entry = client_states.begin(); entry != client_states.end();) {
		if (entry->second.expired()) {
			entry = client_states.erase(entry);
		} else {
			++entry;
		}
	}
}

void DuckherderCatalog::CloseClients() {
	vector<shared_ptr<DuckherderConnectionState>> states;
	unique_ptr<DistributedClient> pending_client;
	{
		concurrency::lock_guard<concurrency::mutex> lock(client_states_mu);
		detached = true;
		pending_client = std::move(attach_client);
		for (auto &entry : client_states) {
			auto state = entry.second.lock();
			if (state) {
				states.emplace_back(std::move(state));
			}
		}
		client_states.clear();
	}
	if (pending_client) {
		pending_client->Close();
	}
	for (auto &state : states) {
		state->Close();
	}
}

} // namespace duckdb
