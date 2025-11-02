#include "distributed_insert.hpp"

#include "distributed_client.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {

struct DistributedInsertGlobalState : public GlobalSinkState {
	idx_t insert_count = 0;
	vector<vector<Value>> collected_rows;
};

struct DistributedInsertLocalState : public LocalSinkState {};

} // namespace

PhysicalDistributedInsert::PhysicalDistributedInsert(PhysicalPlan &physical_plan, TableCatalogEntry &table_p,
                                                     PhysicalOperator &child_operator, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, child_operator.types, estimated_cardinality),
      table(table_p), child(child_operator) {
	children.emplace_back(child);
}

unique_ptr<GlobalSinkState> PhysicalDistributedInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DistributedInsertGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalDistributedInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DistributedInsertLocalState>();
}

SinkResultType PhysicalDistributedInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<DistributedInsertGlobalState>();

	auto &db_instance = DatabaseInstance::GetDatabase(context.client);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Distributed insertion: received %llu rows for table %s",
	                                                 chunk.size(), table.name));

	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		vector<Value> row;
		row.reserve(chunk.ColumnCount());
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
			row.emplace_back(chunk.GetValue(col_idx, row_idx));
		}
		gstate.collected_rows.emplace_back(std::move(row));
	}

	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalDistributedInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                     OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<DistributedInsertGlobalState>();

	auto &db_instance = DatabaseInstance::GetDatabase(context);
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("Distributed insertion finalize: sending %llu rows to server for table %s",
	                                    gstate.insert_count, table.name));

	// TODO(hjiang): We should use arrow as communication protocol instead of a plain sql statement.
	string insert_sql = "INSERT INTO " + table.name + " VALUES ";
	for (idx_t row_idx = 0; row_idx < gstate.collected_rows.size(); row_idx++) {
		if (row_idx > 0)
			insert_sql += ", ";
		insert_sql += "(";

		auto &row = gstate.collected_rows[row_idx];
		for (idx_t col_idx = 0; col_idx < row.size(); col_idx++) {
			if (col_idx > 0)
				insert_sql += ", ";
			// Use ToSQLString() which properly quotes strings
			insert_sql += row[col_idx].ToSQLString();
		}
		insert_sql += ")";
	}

	auto &client = DistributedClient::GetInstance();
	auto result = client.InsertInto(insert_sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::IO, "Failed to insert into server: " + result->GetError());
	}
	return SinkFinalizeType::READY;
}

SourceResultType PhysicalDistributedInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
