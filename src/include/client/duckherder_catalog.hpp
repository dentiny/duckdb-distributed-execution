#pragma once

#include "base_query_recorder.hpp"
#include "distributed.pb.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utils/mutex.hpp"

namespace duckdb {

// Forward declaration.
class DuckCatalog;
class DatabaseInstance;
class DistributedClient;
class DuckherderConnectionState;

// Configuration for remote tables
struct RemoteTableConfig {
	string server_url;
	string remote_table_name;
	bool is_distributed;

	RemoteTableConfig() : is_distributed(false) {
	}
	RemoteTableConfig(string url, string table)
	    : server_url(std::move(url)), remote_table_name(std::move(table)), is_distributed(true) {
	}
};

class DuckherderCatalog : public DuckCatalog {
public:
	DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p, distributed::ClientRole role_p,
	                  connection_t attach_connection_id_p);

	~DuckherderCatalog() override;

	void Initialize(bool load_builtin) override;
	void OnDetach(ClientContext &context) override;

	string GetCatalogType() override {
		return "duckherder";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
	                                              unique_ptr<LogicalOperator> plan,
	                                              unique_ptr<CreateIndexInfo> create_info,
	                                              unique_ptr<AlterTableInfo> alter_info) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;

	bool IsDuckCatalog() override {
		return true;
	}

	bool InMemory() override;
	string GetDBPath() override;
	bool IsEncrypted() const override;
	string GetEncryptionCipher() const override;

	optional_idx GetCatalogVersion(ClientContext &context) override;

	optional_ptr<DependencyManager> GetDependencyManager() override;

	void DropSchema(ClientContext &context, DropInfo &info) override;

	// Remote table management.
	void RegisterRemoteTable(const string &table_name, const string &server_url, const string &remote_table_name);
	void UnregisterRemoteTable(const string &table_name);
	bool IsRemoteTable(const string &table_name) const;
	RemoteTableConfig GetRemoteTableConfig(const string &table_name) const;

	// Get server URL from stored configuration.
	string GetServerUrl() const;

	// Get the remote session owned by this DuckDB connection.
	DistributedClient &GetClient(ClientContext &context);

	// Remote index management.
	void RegisterRemoteIndex(const string &index_name);
	void UnregisterRemoteIndex(const string &index_name);
	bool IsRemoteIndex(const string &index_name) const;

private:
	void CloseClients();
	void EnsureWriteOwner(ClientContext &context) DUCKDB_REQUIRES(client_states_mu);
	shared_ptr<DuckherderConnectionState> GetOrCreateClientState(ClientContext &context)
	    DUCKDB_REQUIRES(client_states_mu);

	concurrency::mutex mu;
	unordered_map<string, unique_ptr<SchemaCatalogEntry>> schema_catalog_entries DUCKDB_GUARDED_BY(mu);

	unique_ptr<DuckCatalog> duckdb_catalog;
	DatabaseInstance &db_instance;

	// Attachment configuration.
	string server_host;
	int server_port;
	distributed::ClientRole role;
	connection_t attach_connection_id;

	// Per-connection remote session state.
	string client_state_key;
	mutable concurrency::mutex client_states_mu;
	bool detached DUCKDB_GUARDED_BY(client_states_mu) = false;
	unique_ptr<DistributedClient> attach_client DUCKDB_GUARDED_BY(client_states_mu);
	unordered_map<connection_t, weak_ptr<DuckherderConnectionState>> client_states DUCKDB_GUARDED_BY(client_states_mu);

	// Remote table configuration.
	// TODO(hjiang): Currently remote tables lives in memory, should provide options to persist and load.
	mutable concurrency::mutex remote_tables_mu;
	unordered_map<string, RemoteTableConfig> remote_tables DUCKDB_GUARDED_BY(remote_tables_mu);

	// Remote index tracking.
	// TODO(hjiang): Currently remote indexes live in memory, should provide options to persist and load.
	mutable concurrency::mutex remote_indexes_mu;
	unordered_set<string> remote_indexes DUCKDB_GUARDED_BY(remote_indexes_mu);
};

} // namespace duckdb
