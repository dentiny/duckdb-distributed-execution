#include "client/execution/remote_create_table_as.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_schema_catalog_entry.hpp"

namespace duckdb {

namespace {

struct RemoteCreateTableAsSourceState : public GlobalSourceState {
	bool executed = false;
	unique_ptr<QueryResult> result;
};

} // namespace

PhysicalRemoteCreateTableAs::PhysicalRemoteCreateTableAs(PhysicalPlan &physical_plan, LogicalCreateTable &op,
                                                         DuckherderCatalog &catalog_p,
                                                         DuckherderSchemaCatalogEntry &schema_p,
                                                         unique_ptr<BoundCreateTableInfo> info_p, string sql_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_TABLE, op.types, op.estimated_cardinality),
      catalog(catalog_p), schema(schema_p), info(std::move(info_p)), sql(std::move(sql_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoteCreateTableAs::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoteCreateTableAsSourceState>();
}

SourceResultType PhysicalRemoteCreateTableAs::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                              OperatorSourceInput &input) const {
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

} // namespace duckdb
