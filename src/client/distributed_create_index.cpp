#include "distributed_create_index.hpp"

#include "distributed_client.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckherder_catalog.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

// Global source state for tracking remote CREATE INDEX execution.
class RemoteCreateIndexGlobalState : public GlobalSourceState {
public:
	RemoteCreateIndexGlobalState() : executed(false) {
	}

	bool executed;
	mutex lock;

	idx_t MaxThreads() override {
		return 1; // Single-threaded execution
	}
};

} // namespace

PhysicalRemoteCreateIndexOperator::PhysicalRemoteCreateIndexOperator(PhysicalPlan &physical_plan,
                                                                     unique_ptr<CreateIndexInfo> info_p,
                                                                     string catalog_name_p, string schema_name_p,
                                                                     string table_name_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)), catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)),
      table_name(std::move(table_name_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoteCreateIndexOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoteCreateIndexGlobalState>();
}

SourceResultType PhysicalRemoteCreateIndexOperator::GetData(ExecutionContext &context, DataChunk &chunk,
                                                            OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<RemoteCreateIndexGlobalState>();
	auto &db_instance = DatabaseInstance::GetDatabase(context.client);

	// Execute the CREATE INDEX on the remote server and register it locally.
	lock_guard<mutex> lock(gstate.lock);
	if (gstate.executed) {
		return SourceResultType::FINISHED;
	}

	// Get the schema and table to create the catalog entry.
	auto &catalog = Catalog::GetCatalog(context.client, catalog_name);
	auto &schema = catalog.GetSchema(context.client, schema_name);
	auto &table = catalog.GetEntry<TableCatalogEntry>(context.client, schema_name, table_name);

	// Generate CREATE INDEX SQL and remove catalog prefix for remote execution.
	string create_sql = SanitizeQuery(info->ToString(), catalog_name);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing CREATE INDEX on remote server: %s", create_sql));

	// Execute on remote server using the singleton DistributedClient
	auto &client = DistributedClient::GetInstance();
	auto result = client.ExecuteSQL(create_sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::CATALOG, "Failed to create index on server: " + result->GetError());
	}

	// Create local catalog entry for tracking, and register the index as remote.
	auto transaction = catalog.GetCatalogTransaction(context.client);
	auto index_entry = schema.CreateIndex(transaction, *info, table);
	if (index_entry == nullptr) {
		throw Exception(ExceptionType::CATALOG,
		                StringUtil::Format("Failed to create catalog entry for index %s", info->index_name));
	}

	gstate.executed = true;
	return SourceResultType::FINISHED;
}

} // namespace duckdb
