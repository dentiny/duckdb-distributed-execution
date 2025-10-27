#include "physical_distributed_insert.hpp"
#include "distributed_server.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/logger.hpp"
#include <iostream>

namespace duckdb {

struct DistributedInsertGlobalState : public GlobalSinkState {
	idx_t insert_count = 0;
	vector<vector<Value>> collected_rows;
};

struct DistributedInsertLocalState : public LocalSinkState {
	// Empty for now
};

PhysicalDistributedInsert::PhysicalDistributedInsert(PhysicalPlan &physical_plan, TableCatalogEntry &table,
                                                     PhysicalOperator &child_operator, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, child_operator.types, estimated_cardinality),
      table(table), child(child_operator) {
	// Add the child operator
	children.push_back(child);
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

	auto &db = DatabaseInstance::GetDatabase(context.client);
	DUCKDB_LOG_DEBUG(
	    db, StringUtil::Format("💾 Distributed INSERT: Received %llu rows for table %s", chunk.size(), table.name));

	// Collect the rows to send to server
	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		vector<Value> row;
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			row.push_back(chunk.GetValue(col_idx, row_idx));
		}
		gstate.collected_rows.push_back(std::move(row));
	}

	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalDistributedInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                     OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<DistributedInsertGlobalState>();

	auto &db = DatabaseInstance::GetDatabase(context);
	DUCKDB_LOG_DEBUG(db, StringUtil::Format("💾 Distributed INSERT Finalize: Sending %llu rows to server for table %s",
	                                        gstate.insert_count, table.name));

	// Build INSERT SQL
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

	std::cout << "📡 Sending INSERT to server: " << insert_sql.substr(0, 100) << "..." << std::endl;

	// Send to server
	auto &server = DistributedServer::GetInstance();
	auto result = server.InsertInto(insert_sql);

	if (result->HasError()) {
		std::cerr << "❌ Server INSERT failed: " << result->GetError() << std::endl;
		throw Exception(ExceptionType::IO, "Failed to insert into server: " + result->GetError());
	}

	std::cout << "   ✅ Inserted " << gstate.insert_count << " rows on server successfully!" << std::endl;

	return SinkFinalizeType::READY;
}

SourceResultType PhysicalDistributedInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	// INSERT doesn't return data (unless RETURNING is used, which we don't support yet)
	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
