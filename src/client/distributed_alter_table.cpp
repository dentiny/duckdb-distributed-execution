#include "distributed_alter_table.hpp"

#include "distributed_client.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "motherduck_catalog.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

// Global source state for tracking remote ALTER TABLE execution.
class RemoteAlterTableGlobalState : public GlobalSourceState {
public:
	RemoteAlterTableGlobalState() : executed(false) {
	}

	bool executed;
	mutex lock;

	idx_t MaxThreads() override {
		return 1; // Single-threaded execution
	}
};

} // namespace

PhysicalRemoteAlterTableOperator::PhysicalRemoteAlterTableOperator(PhysicalPlan &physical_plan,
                                                                   unique_ptr<AlterTableInfo> info_p,
                                                                   string catalog_name_p, string schema_name_p,
                                                                   string table_name_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)), catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)),
      table_name(std::move(table_name_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoteAlterTableOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoteAlterTableGlobalState>();
}

SourceResultType PhysicalRemoteAlterTableOperator::GetData(ExecutionContext &context, DataChunk &chunk,
                                                           OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<RemoteAlterTableGlobalState>();
	auto &db_instance = DatabaseInstance::GetDatabase(context.client);

	// Execute the ALTER TABLE on the remote server.
	lock_guard<mutex> lock(gstate.lock);
	if (gstate.executed) {
		return SourceResultType::FINISHED;
	}

	// Generate ALTER TABLE SQL and remove catalog prefix for remote execution.
	string alter_sql = SanitizeQuery(info->ToString(), catalog_name);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing ALTER TABLE on remote server: %s", alter_sql));

	// Execute on remote server using the singleton DistributedClient
	auto &client = DistributedClient::GetInstance();
	auto result = client.ExecuteSQL(alter_sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::CATALOG, "Failed to alter table on server: " + result->GetError());
	}

	// Now apply the alteration locally to update the catalog.
	auto &catalog = Catalog::GetCatalog(context.client, catalog_name);
	catalog.Alter(context.client, *info);

	gstate.executed = true;
	return SourceResultType::FINISHED;
}

} // namespace duckdb

