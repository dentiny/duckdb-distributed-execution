#include "duckherder_catalog.hpp"

#include "client/duckherder_connection_state.hpp"
#include "client/execution/distributed_client.hpp"
#include "client/execution/remote_dml.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
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
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
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

vector<vector<Value>> FetchRows(DistributedClient &client, const string &sql, const vector<LogicalType> &types) {
	auto result = client.ScanTable(sql, NO_QUERY_LIMIT, NO_QUERY_OFFSET, &types);
	if (result->HasError()) {
		throw IOException("Failed to load remote catalog metadata: %s", result->GetError());
	}
	vector<vector<Value>> rows;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			vector<Value> row;
			row.reserve(chunk->ColumnCount());
			for (idx_t column_idx = 0; column_idx < chunk->ColumnCount(); column_idx++) {
				row.push_back(chunk->GetValue(column_idx, row_idx));
			}
			rows.push_back(std::move(row));
		}
	}
	return rows;
}

string QuotedIdentifier(const string &name) {
	return KeywordHelper::WriteQuoted(name, '"');
}

string QualifiedMainName(const string &name) {
	return QuotedIdentifier(DEFAULT_SCHEMA) + "." + QuotedIdentifier(name);
}

} // namespace

DuckherderCatalog::DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p,
                                     distributed::ClientRole role_p, connection_t attach_connection_id_p)
    : DuckCatalog(db), duckdb_catalog(make_uniq<DuckCatalog>(db)), db_instance(db.GetDatabase()),
      server_host(std::move(server_host_p)), server_port(server_port_p), role(role_p),
      attach_connection_id(attach_connection_id_p),
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

void DuckherderCatalog::Initialize(bool load_builtin) {
	duckdb_catalog->Initialize(load_builtin);
}

void DuckherderCatalog::FinalizeLoad(optional_ptr<ClientContext> context) {
	duckdb_catalog->FinalizeLoad(context);
	if (context) {
		LoadRemoteCatalog(*context);
	}
}

void DuckherderCatalog::LoadRemoteCatalog(ClientContext &context) {
	auto &client = GetClient(context);

	auto type_rows = FetchRows(client,
	                           "SELECT type_name, labels FROM duckdb_types() "
	                           "WHERE database_name = current_database() AND schema_name = 'main' AND NOT internal "
	                           "AND labels IS NOT NULL ORDER BY type_name",
	                           {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)});
	for (auto &row : type_rows) {
		vector<string> labels;
		for (auto &label : ListValue::GetChildren(row[1])) {
			labels.push_back(label.ToSQLString());
		}
		auto sql = StringUtil::Format("CREATE TYPE %s AS ENUM (%s)", QualifiedMainName(row[0].GetValue<string>()),
		                              StringUtil::Join(labels, ", "));
		auto info = ParseCreateInfo(sql);
		auto &type_info = info->Cast<CreateTypeInfo>();
		type_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto transaction = CatalogTransaction::GetSystemTransaction(db_instance);
		auto &schema = duckdb_catalog->GetSchema(transaction, DEFAULT_SCHEMA);
		schema.CreateType(transaction, type_info);
	}

	auto table_rows = FetchRows(client,
	                            "SELECT table_name, sql FROM duckdb_tables() "
	                            "WHERE database_name = current_database() AND schema_name = 'main' AND NOT internal "
	                            "ORDER BY table_name",
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR});
	for (auto &row : table_rows) {
		auto info = unique_ptr_cast<CreateInfo, CreateTableInfo>(ParseCreateInfo(row[1].GetValue<string>()));
		info->schema = DEFAULT_SCHEMA;
		info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto transaction = CatalogTransaction::GetSystemTransaction(db_instance);
		auto &schema = duckdb_catalog->GetSchema(transaction, DEFAULT_SCHEMA);
		auto binder = Binder::CreateBinder(context);
		auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);
		duckdb_catalog->CreateTable(transaction, schema, *bound_info);
		auto table_name = row[0].GetValue<string>();
		RegisterRemoteTable(table_name, GetServerUrl(), QuotedIdentifier(table_name));
	}
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

	concurrency::lock_guard<concurrency::mutex> lck(mu);
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
	return duckdb_catalog->PlanCreateTableAs(context, planner, op, plan);
}

PhysicalOperator &DuckherderCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanInsert");

	// Attempt insertion into remote table if registered.
	bool is_remote = IsRemoteTable(op.table.name);
	if (is_remote) {
		auto sql = GetRemoteStatementSQL(context);
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push INSERT to control node: %s", sql));
		return planner.Make<PhysicalRemoteDML>(PhysicalOperatorType::INSERT, op.types, op.table, std::move(sql),
		                                       op.estimated_cardinality);
	}

	// Fallback to local insertion.
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Execute local insertion to table %s", op.table.name));
	return duckdb_catalog->PlanInsert(context, planner, op, plan);
}

PhysicalOperator &DuckherderCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanDelete");

	// Attempt deletion from remote table if registered.
	bool is_remote = IsRemoteTable(op.table.name);
	if (is_remote) {
		auto sql = GetRemoteStatementSQL(context);
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push DELETE to control node: %s", sql));
		return planner.Make<PhysicalRemoteDML>(PhysicalOperatorType::DELETE_OPERATOR, op.types, op.table,
		                                       std::move(sql), op.estimated_cardinality);
	}

	// Fallback to local deletion.
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Execute local deletion from table %s", op.table.name));
	return duckdb_catalog->PlanDelete(context, planner, op, plan);
}

PhysicalOperator &DuckherderCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderCatalog::PlanUpdate");
	if (IsRemoteTable(op.table.name)) {
		auto sql = GetRemoteStatementSQL(context);
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Push UPDATE to control node: %s", sql));
		return planner.Make<PhysicalRemoteDML>(PhysicalOperatorType::UPDATE, op.types, op.table, std::move(sql),
		                                       op.estimated_cardinality);
	}
	return duckdb_catalog->PlanUpdate(context, planner, op, plan);
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
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
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
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
	const size_t count = remote_tables.erase(table_name);
	if (count != 1) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to unregister table %s because it hasn't been registered!", table_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Unregistered remote table %s", table_name));
}

bool DuckherderCatalog::IsRemoteTable(const string &table_name) const {
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
	auto it = remote_tables.find(table_name);
	bool found = it != remote_tables.end() && it->second.is_distributed;
	return found;
}

RemoteTableConfig DuckherderCatalog::GetRemoteTableConfig(const string &table_name) const {
	concurrency::lock_guard<concurrency::mutex> lck(remote_tables_mu);
	auto it = remote_tables.find(table_name);
	if (it != remote_tables.end()) {
		return it->second;
	}
	// Fallbacks to default, which is not distributed table.
	return RemoteTableConfig();
}

void DuckherderCatalog::RegisterRemoteIndex(const string &index_name) {
	concurrency::lock_guard<concurrency::mutex> lck(remote_indexes_mu);
	const bool succ = remote_indexes.insert(index_name).second;
	if (!succ) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to register index %s because it's already registered!", index_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Registered remote index %s", index_name));
}

void DuckherderCatalog::UnregisterRemoteIndex(const string &index_name) {
	concurrency::lock_guard<concurrency::mutex> lck(remote_indexes_mu);
	const size_t count = remote_indexes.erase(index_name);
	if (count != 1) {
		throw InvalidInputException(
		    StringUtil::Format("Failed to unregister index %s because it hasn't been registered!", index_name));
	}
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Unregistered remote index %s", index_name));
}

bool DuckherderCatalog::IsRemoteIndex(const string &index_name) const {
	concurrency::lock_guard<concurrency::mutex> lck(remote_indexes_mu);
	return remote_indexes.find(index_name) != remote_indexes.end();
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
