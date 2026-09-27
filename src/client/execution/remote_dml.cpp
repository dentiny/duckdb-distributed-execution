#include "client/execution/remote_dml.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

struct RemoteDMLSourceState : public GlobalSourceState {
	bool executed = false;
};

} // namespace

PhysicalRemoteDML::PhysicalRemoteDML(PhysicalPlan &physical_plan, PhysicalOperatorType operator_type,
                                     vector<LogicalType> types, TableCatalogEntry &table_p, string sql_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, operator_type, std::move(types), estimated_cardinality), table(table_p),
      sql(std::move(sql_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoteDML::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoteDMLSourceState>();
}

SourceResultType PhysicalRemoteDML::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<RemoteDMLSourceState>();
	if (state.executed) {
		return SourceResultType::FINISHED;
	}
	state.executed = true;

	auto result = GetDistributedClient(table).ExecuteStatement(sql, table.catalog.GetName());
	if (result->HasError()) {
		throw Exception(ExceptionType::IO, "Failed to execute DML on control node: " + result->GetError());
	}
	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
