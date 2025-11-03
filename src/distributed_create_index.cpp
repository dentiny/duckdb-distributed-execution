#include "distributed_create_index.hpp"

#include "distributed_client.hpp"
#include "distributed_protocol.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "motherduck_catalog.hpp"

namespace duckdb {

PhysicalRemoteCreateIndex::PhysicalRemoteCreateIndex(PhysicalPlan &physical_plan, unique_ptr<CreateIndexInfo> info_p,
                                                     string catalog_name_p, string schema_name_p, string table_name_p,
                                                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)), catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)),
      table_name(std::move(table_name_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoteCreateIndex::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoteCreateIndexGlobalState>();
}

SourceResultType PhysicalRemoteCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<RemoteCreateIndexGlobalState>();
	auto &db_instance = DatabaseInstance::GetDatabase(context.client);

	// Execute the CREATE INDEX on the remote server and register it locally (only once)
	lock_guard<mutex> lock(gstate.lock);
	if (!gstate.executed) {
		// Get the schema and table to create the catalog entry
		auto &catalog = Catalog::GetCatalog(context.client, catalog_name);
		auto &schema = catalog.GetSchema(context.client, schema_name);
		auto &table = catalog.GetEntry<TableCatalogEntry>(context.client, schema_name, table_name);

		// Generate CREATE INDEX SQL
		string create_sql = info->ToString();

		// Remove the catalog prefix (e.g., "md.") from the table name since the remote server
		// doesn't have that catalog
		string catalog_prefix = catalog_name + ".";
		auto pos = create_sql.find(catalog_prefix);
		if (pos != string::npos) {
			create_sql.erase(pos, catalog_prefix.length());
		}

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing CREATE INDEX on remote server: %s", create_sql));

		// Execute on remote server using the singleton DistributedClient
		auto &client = DistributedClient::GetInstance();
		auto result = client.ExecuteSQL(create_sql);
		if (result->HasError()) {
			throw Exception(ExceptionType::CATALOG, "Failed to create index on server: " + result->GetError());
		}

		// Create the catalog entry for tracking
		// This will also register the index as remote
		auto transaction = catalog.GetCatalogTransaction(context.client);
		auto index_entry = schema.CreateIndex(transaction, *info, table);

		if (!index_entry) {
			throw Exception(ExceptionType::CATALOG, "Failed to create index catalog entry");
		}

		gstate.executed = true;
	}

	// Return finished - no data to return
	return SourceResultType::FINISHED;
}

} // namespace duckdb
