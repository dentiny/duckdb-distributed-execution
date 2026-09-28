#pragma once

#include "client/duckherder_remote_table_config.hpp"
#include "distributed.pb.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utils/mutex.hpp"

namespace duckdb {

// Forward declaration.
class DuckCatalog;
class DatabaseInstance;
class DistributedClient;
class DuckherderConnectionState;
class DuckherderPragmas;
class DuckherderSchemaCatalogEntry;
class DuckherderTableCatalogEntry;

class DuckherderCatalog : public DuckCatalog {
public:
	DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p, distributed::ClientRole role_p,
	                  connection_t attach_connection_id_p);

	~DuckherderCatalog() override;

	void FinalizeLoad(optional_ptr<ClientContext> context) override;
	void OnDetach(ClientContext &context) override;

	string GetCatalogType() override {
		return "duckherder";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	void DropSchema(ClientContext &context, DropInfo &info) override;

	// Get the remote session owned by this DuckDB connection.
	DistributedClient &GetClient(ClientContext &context);

private:
	friend class DuckherderPragmas;
	friend class DuckherderSchemaCatalogEntry;
	friend class DuckherderTableCatalogEntry;

	void RegisterRemoteTable(const string &table_name, const string &server_url, const string &remote_table_name);
	void UnregisterRemoteTable(const string &table_name);
	bool IsRemoteTable(const string &schema_name, const string &table_name) const;
	RemoteTableConfig GetRemoteTableConfig(const string &schema_name, const string &table_name) const;
	string GetServerUrl() const;

	optional_ptr<CatalogEntry> CreateSchemaLocal(CatalogTransaction transaction, CreateSchemaInfo &info);
	void LoadRemoteCatalog(ClientContext &context);
	void CloseClients();
	void EnsureWriteOwner(ClientContext &context) DUCKDB_REQUIRES(client_states_mu);
	shared_ptr<DuckherderConnectionState> GetOrCreateClientState(ClientContext &context)
	    DUCKDB_REQUIRES(client_states_mu);
	void PruneExpiredClientStates() DUCKDB_REQUIRES(client_states_mu);

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

	// Explicit routing overrides registered through the compatibility pragmas.
	mutable concurrency::mutex remote_tables_mu;
	RemoteTableMap remote_tables DUCKDB_GUARDED_BY(remote_tables_mu);
};

} // namespace duckdb
