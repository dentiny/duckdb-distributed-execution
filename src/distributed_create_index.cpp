#include "distributed_create_index.hpp"

#include "distributed_client.hpp"
#include "distributed_protocol.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "motherduck_catalog.hpp"

namespace duckdb {

PhysicalRemoteCreateIndex::PhysicalRemoteCreateIndex(PhysicalPlan &physical_plan, unique_ptr<CreateIndexInfo> info_p,
                                                     string catalog_name_p, string schema_name_p, string table_name_p,
                                                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)), catalog_name(std::move(catalog_name_p)),
      schema_name(std::move(schema_name_p)), table_name(std::move(table_name_p)) {
	std::cerr << "[PhysicalRemoteCreateIndex::Constructor] this=" << this 
	          << " index_name=" << info->index_name << std::endl;
}

unique_ptr<GlobalSourceState> PhysicalRemoteCreateIndex::GetGlobalSourceState(ClientContext &context) const {
	auto state = make_uniq<RemoteCreateIndexGlobalState>();
	std::cerr << "[GetGlobalSourceState] this=" << this << " state=" << state.get() 
	          << " index_name=" << info->index_name << std::endl;
	return state;
}

SourceResultType PhysicalRemoteCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<RemoteCreateIndexGlobalState>();
	
	std::cerr << "[PhysicalRemoteCreateIndex::GetData] Called - gstate ptr=" << &gstate 
	          << " executed=" << gstate.executed << std::endl;
	
	// Execute the CREATE INDEX on the remote server and register it locally (only once)
	lock_guard<mutex> lock(gstate.lock);
	if (!gstate.executed) {
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] Starting execution..." << std::endl;
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] catalog_name=" << catalog_name << std::endl;
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] schema_name=" << schema_name << std::endl;
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] table_name=" << table_name << std::endl;
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] info->schema=" << info->schema << std::endl;
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] info->table=" << info->table << std::endl;
		
		try {
			// Get the schema and table to create the catalog entry
			std::cerr << "[CLIENT] Getting catalog..." << std::endl;
			auto &catalog = Catalog::GetCatalog(context.client, catalog_name);
			
			std::cerr << "[CLIENT] Getting schema..." << std::endl;
			auto &schema = catalog.GetSchema(context.client, schema_name);
			
			std::cerr << "[CLIENT] Getting table..." << std::endl;
			auto &table = catalog.GetEntry<TableCatalogEntry>(context.client, schema_name, table_name);
			
			std::cerr << "[CLIENT] Got catalog, schema, and table successfully" << std::endl;
			
			// Generate CREATE INDEX SQL
			string create_sql = info->ToString();
			std::cerr << "[CLIENT] Generated SQL (before cleanup): " << create_sql << std::endl;
			
			// Remove the catalog prefix (e.g., "md.") from the table name since the remote server
			// doesn't have that catalog
			string catalog_prefix = catalog_name + ".";
			auto pos = create_sql.find(catalog_prefix);
			if (pos != string::npos) {
				create_sql.erase(pos, catalog_prefix.length());
			}
			
			std::cerr << "[SERVER] Executing SQL on remote server: " << create_sql << std::endl;
			
			// Execute on remote server using the singleton DistributedClient
			auto &client = DistributedClient::GetInstance();
			auto result = client.ExecuteSQL(create_sql);
			if (result->HasError()) {
				std::cerr << "[SERVER] ERROR from server: " << result->GetError() << std::endl;
				throw Exception(ExceptionType::CATALOG, "Failed to create index on server: " + result->GetError());
			}
			
			std::cerr << "[SERVER] Remote execution succeeded!" << std::endl;
			std::cerr << "[CLIENT] Creating catalog entry and registering..." << std::endl;
			
			// Create the catalog entry for tracking
			// This will also register the index as remote
			auto transaction = catalog.GetCatalogTransaction(context.client);
			auto index_entry = schema.CreateIndex(transaction, *info, table);
			
			std::cerr << "[CLIENT] Catalog entry created: " << (index_entry ? "YES" : "NO") << std::endl;
			
		} catch (std::exception &e) {
			std::cerr << "[EXCEPTION] " << e.what() << std::endl;
			throw;
		}

		gstate.executed = true;
		std::cerr << "[PhysicalRemoteCreateIndex::GetData] Execution complete, marked as executed" << std::endl;
	}

	// Return finished - no data to return
	return SourceResultType::FINISHED;
}

} // namespace duckdb

