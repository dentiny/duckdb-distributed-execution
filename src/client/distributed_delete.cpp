#include "distributed_delete.hpp"

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

struct DistributedDeleteGlobalState : public GlobalSinkState {
	idx_t delete_count = 0;
	// Store the actual data chunks with column values, not just row IDs.
	vector<unique_ptr<DataChunk>> collected_chunks;
};

struct DistributedDeleteLocalState : public LocalSinkState {};

} // namespace

PhysicalDistributedDelete::PhysicalDistributedDelete(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                     TableCatalogEntry &table_p, PhysicalOperator &child_operator,
                                                     idx_t row_id_index_p, idx_t estimated_cardinality,
                                                     bool return_chunk_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::DELETE_OPERATOR, std::move(types), estimated_cardinality),
      table(table_p), child(child_operator), row_id_index(row_id_index_p), return_chunk(return_chunk_p) {
	children.emplace_back(child);
}

unique_ptr<GlobalSinkState> PhysicalDistributedDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DistributedDeleteGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalDistributedDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DistributedDeleteLocalState>();
}

SinkResultType PhysicalDistributedDelete::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<DistributedDeleteGlobalState>();

	auto &db_instance = DatabaseInstance::GetDatabase(context.client);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Distributed deletion: received %llu rows for table %s",
	                                                 chunk.size(), table.name));

	auto stored_chunk = make_uniq<DataChunk>();
	stored_chunk->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	chunk.Copy(*stored_chunk);
	gstate.collected_chunks.push_back(std::move(stored_chunk));

	gstate.delete_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalDistributedDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                     OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<DistributedDeleteGlobalState>();

	auto &db_instance = DatabaseInstance::GetDatabase(context);
	DUCKDB_LOG_DEBUG(
	    db_instance,
	    StringUtil::Format("Distributed deletion finalize: sending %llu row deletions to server for table %s",
	                       gstate.delete_count, table.name));

	// TODO(hjiang): Simplified implementation using the first column as a key.
	// In a real distributed system, you'd forward the original WHERE clause SQL.
	// For now, we reconstruct the DELETE using the first column values.
	if (gstate.collected_chunks.empty() || gstate.delete_count == 0) {
		return SinkFinalizeType::READY;
	}

	// TODO(hjiang): A simplified implementation which builds DELETE statement using the first column values.
	auto &first_chunk = *gstate.collected_chunks[0];
	if (first_chunk.ColumnCount() == 0) {
		return SinkFinalizeType::READY;
	}

	// Get the table columns to find the first column name
	auto &columns = table.GetColumns();
	if (columns.LogicalColumnCount() == 0) {
		return SinkFinalizeType::READY;
	}

	string key_column = columns.GetColumn(LogicalIndex(0)).Name();

	// Collect all key values from all chunks.
	vector<Value> key_values;
	for (auto &chunk_ptr : gstate.collected_chunks) {
		auto &chunk = *chunk_ptr;
		auto &key_vector = chunk.data[0]; // First column
		for (idx_t i = 0; i < chunk.size(); i++) {
			key_values.push_back(key_vector.GetValue(i));
		}
	}

	// Build DELETE statement.
	string delete_sql = "DELETE FROM " + table.name + " WHERE " + key_column + " IN (";
	for (idx_t i = 0; i < key_values.size(); i++) {
		if (i > 0) {
			delete_sql += ", ";
		}
		delete_sql += key_values[i].ToSQLString();
	}
	delete_sql += ")";
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Executing DELETE on remote server: %s", delete_sql));

	auto &client = DistributedClient::GetInstance();
	auto result = client.ExecuteSQL(delete_sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::IO, "Failed to delete from server: " + result->GetError());
	}
	return SinkFinalizeType::READY;
}

SourceResultType PhysicalDistributedDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	// TODO(hjiang): Implement return chunk.
	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
