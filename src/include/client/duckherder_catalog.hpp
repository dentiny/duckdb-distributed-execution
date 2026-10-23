#pragma once

#include "duckdb/common/identifier.hpp"
#include "client/duckherder_client_sessions.hpp"
#include "distributed.pb.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "storage_config.pb.h"
#include "utils/mutex.hpp"

namespace duckdb {

// Forward declaration.
class DuckCatalog;
class DatabaseInstance;
class DistributedClient;
class DuckherderPragmas;
class DuckherderSchemaCatalogEntry;
class DuckherderTableCatalogEntry;

class DuckherderCatalog : public DuckCatalog {
public:
	DuckherderCatalog(AttachedDatabase &db, string server_host_p, int server_port_p, distributed::ClientRole role_p,
	                  connection_t attach_connection_id_p, distributed::StorageConfig storage_config_p);

	~DuckherderCatalog() override;

	void FinalizeLoad(optional_ptr<ClientContext> context) override;
	void OnDetach(ClientContext &context) override;

	string GetCatalogType() override {
		return "duckherder";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	// Executes CREATE TABLE AS on the control node and caches the created table metadata locally.
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	// Forwards MERGE, INSERT ... ON CONFLICT, and INSERT OR REPLACE to the control node.
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;
	// Routes ADD PRIMARY KEY to the control node without building a client-side index over a remote scan.
	unique_ptr<LogicalOperator> BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
	                                              unique_ptr<LogicalOperator> plan,
	                                              unique_ptr<CreateIndexInfo> create_info,
	                                              unique_ptr<AlterTableInfo> alter_info) override;

	void DropSchema(ClientContext &context, DropInfo &info) override;

	// Lets DuckDB's RemotePushdownOptimizer run queries, writes and CREATE TABLE AS that only use this database on its
	// server as a whole, and CONNECT route a connection's SQL to the server.
	bool Supports(RemoteCapability capability) const override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	bool SupportsPushdown(const ParsedExpression &expression) override;
	bool SupportsPushdown(const TableRef &ref) override;
	bool SupportsPushdown(const QueryNode &node) override;
	bool SupportsPushdown(const SQLStatement &statement) override;

	// Get the remote session owned by this DuckDB connection.
	DistributedClient &GetClient(ClientContext &context);
	// Return no client when this attachment is unavailable to the connection.
	optional_ptr<DistributedClient> TryGetClient(ClientContext &context);

	// Returns the remote row count captured when the catalog was loaded, if known.
	optional_idx GetEstimatedCardinality(const Identifier &schema_name, const Identifier &table_name) const;

private:
	friend class DuckherderCatalogLoader;
	friend class DuckherderPragmas;
	friend class DuckherderSchemaCatalogEntry;
	friend class DuckherderTableCatalogEntry;

	string GetServerUrl() const;

	optional_ptr<CatalogEntry> CreateSchemaLocal(CatalogTransaction transaction, CreateSchemaInfo &info);
	void SetEstimatedCardinality(const Identifier &schema_name, const Identifier &table_name, idx_t cardinality);

	DatabaseInstance &db_instance;

	// Attachment configuration.
	string server_host;
	int server_port;

	DuckherderClientSessions client_sessions;

	// Maps from a qualified remote table name to its row count when the catalog was loaded.
	// Only guides join ordering, so it is not refreshed on later writes.
	mutable concurrency::mutex table_cardinalities_mu;
	case_insensitive_map_t<idx_t> table_cardinalities DUCKDB_GUARDED_BY(table_cardinalities_mu);
};

} // namespace duckdb
