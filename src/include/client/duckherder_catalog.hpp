#pragma once

#include <mutex>

#include "base_query_recorder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

// Forward declaration.
class DuckCatalog;
class DatabaseInstance;

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
	explicit DuckherderCatalog(AttachedDatabase &db, string server_host = "localhost", int server_port = 8815,
	                           string server_db_path = "");

	~DuckherderCatalog() override;

	void Initialize(bool load_builtin) override;

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
	
	// Get server URL from stored configuration
	string GetServerUrl() const;

	// Remote index management.
	void RegisterRemoteIndex(const string &index_name);
	void UnregisterRemoteIndex(const string &index_name);
	bool IsRemoteIndex(const string &index_name) const;

private:
	std::mutex mu;
	unordered_map<string, unique_ptr<SchemaCatalogEntry>> schema_catalog_entries;

	unique_ptr<DuckCatalog> duckdb_catalog;
	DatabaseInstance &db_instance;

	// Server configuration from ATTACH DATABASE
	string server_host_;
	int server_port_;
	string server_db_path_;

	// Remote table configuration.
	// TODO(hjiang): Currently remote tables lives in memory, should provide options to persist and load.
	mutable std::mutex remote_tables_mu;
	unordered_map<string, RemoteTableConfig> remote_tables;

	// Remote index tracking.
	// TODO(hjiang): Currently remote indexes live in memory, should provide options to persist and load.
	mutable std::mutex remote_indexes_mu;
	unordered_set<string> remote_indexes;
};

} // namespace duckdb
