#include "server/driver/worker_fragment_pushdown.hpp"

#include "arrow_utils.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "server/driver/distributed_executor.hpp"
#include "utils/sql_render_utils.hpp"

#include <chrono>

namespace duckdb {

namespace {

struct WorkerFragmentBindData : public TableFunctionData {
	WorkerFragmentBindData(string sql_p, vector<LogicalType> types_p)
	    : sql(std::move(sql_p)), types(std::move(types_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<WorkerFragmentBindData>(sql, types);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<WorkerFragmentBindData>();
		return other.sql == sql && other.types == types;
	}

	string sql;
	vector<LogicalType> types;
};

struct WorkerFragmentGlobalState : public GlobalTableFunctionState {
	vector<unique_ptr<DataChunk>> chunks;
	idx_t chunk_idx = 0;
	idx_t chunk_offset = 0;
};

unique_ptr<GlobalTableFunctionState> WorkerFragmentInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<WorkerFragmentBindData>();
	auto result = make_uniq<WorkerFragmentGlobalState>();
	result->chunks = context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME)
	                     ->Execute(context, bind_data.sql, bind_data.types);
	return std::move(result);
}

void WorkerFragmentExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<WorkerFragmentGlobalState>();
	while (state.chunk_idx < state.chunks.size() && state.chunk_offset == state.chunks[state.chunk_idx]->size()) {
		state.chunks[state.chunk_idx++].reset();
		state.chunk_offset = 0;
	}
	if (state.chunk_idx == state.chunks.size()) {
		output.SetCardinality(0);
		return;
	}
	auto &chunk = *state.chunks[state.chunk_idx];
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, chunk.size() - state.chunk_offset);
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); ++col_idx) {
		VectorOperations::Copy(chunk.data[col_idx], output.data[col_idx], state.chunk_offset + count,
		                       state.chunk_offset, /*target_offset=*/0);
	}
	output.SetCardinality(count);
	state.chunk_offset += count;
}

string GetTableColumn(TableCatalogEntry &table, column_t column_id, LogicalType &type) {
	if (IsVirtualColumn(column_id)) {
		auto virtual_columns = table.GetVirtualColumns();
		auto entry = virtual_columns.find(column_id);
		if (entry == virtual_columns.end()) {
			throw InternalException("Table %s has no virtual column %llu", table.name, column_id);
		}
		type = entry->second.type;
		return KeywordHelper::WriteOptionallyQuoted(entry->second.name);
	}
	auto &column = table.GetColumn(LogicalIndex(column_id));
	type = column.Type();
	return KeywordHelper::WriteOptionallyQuoted(column.Name());
}

// Returns the table `get` scans if workers can partition it, or nullptr.
optional_ptr<TableCatalogEntry> GetPartitionedTable(LogicalGet &get) {
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || get.extra_info.sample_options) {
		return nullptr;
	}
	return table;
}

string GetTableName(TableCatalogEntry &table) {
	return KeywordHelper::WriteOptionallyQuoted(table.schema.name) + "." +
	       KeywordHelper::WriteOptionallyQuoted(table.name);
}

unique_ptr<LogicalGet> CreateFragment(Binder &binder, string sql, vector<LogicalType> types, vector<string> names) {
	TableFunction function("worker_fragment", {}, WorkerFragmentExecute, nullptr, WorkerFragmentInitGlobal);
	auto bind_data = make_uniq<WorkerFragmentBindData>(std::move(sql), types);
	auto result = make_uniq<LogicalGet>(binder.GenerateTableIndex(), std::move(function), std::move(bind_data),
	                                    std::move(types), std::move(names));
	vector<ColumnIndex> column_ids;
	for (idx_t idx = 0; idx < result->returned_types.size(); ++idx) {
		column_ids.emplace_back(idx);
	}
	result->SetColumnIds(std::move(column_ids));
	return result;
}

unique_ptr<LogicalOperator> TryCreateAggregateFragment(Binder &binder, LogicalAggregate &aggregate,
                                                       vector<ReplacementBinding> &replacements) {
	auto get = GetAggregateScan(aggregate);
	auto table = get ? GetPartitionedTable(*get) : nullptr;
	if (!table) {
		return nullptr;
	}
	vector<LogicalType> types;
	vector<string> names;
	auto sql = RenderAggregateQuery(
	    aggregate, [&](column_t column_id, LogicalType &type) { return GetTableColumn(*table, column_id, type); },
	    GetTableName(*table), types, names);
	if (sql.empty()) {
		return nullptr;
	}
	auto fragment = CreateFragment(binder, std::move(sql), std::move(types), std::move(names));
	const auto group_count = aggregate.groups.size();
	for (idx_t idx = 0; idx < group_count; ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.group_index, idx), ColumnBinding(fragment->table_index, idx));
	}
	for (idx_t idx = 0; idx < aggregate.expressions.size(); ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.aggregate_index, idx),
		                          ColumnBinding(fragment->table_index, group_count + idx));
	}
	return std::move(fragment);
}

unique_ptr<LogicalOperator> TryCreateScanFragment(Binder &binder, LogicalGet &get,
                                                  vector<ReplacementBinding> &replacements) {
	auto table = GetPartitionedTable(get);
	auto &column_ids = get.GetColumnIds();
	if (!table || column_ids.empty()) {
		return nullptr;
	}
	auto get_column = [&](column_t column_id, LogicalType &type) {
		return GetTableColumn(*table, column_id, type);
	};
	auto bindings = get.GetColumnBindings();
	vector<string> select_list;
	vector<LogicalType> types;
	vector<string> names;
	for (auto &binding : bindings) {
		auto &column = column_ids[binding.column_index];
		if (column.HasChildren() || column.GetPrimaryIndex() == COLUMN_IDENTIFIER_EMPTY) {
			return nullptr;
		}
		LogicalType type;
		select_list.emplace_back(get_column(column.GetPrimaryIndex(), type));
		types.emplace_back(std::move(type));
		names.emplace_back(select_list.back());
	}
	vector<string> predicates;
	if (!RenderScanFilters(get, get_column, predicates)) {
		return nullptr;
	}
	auto sql = StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), GetTableName(*table));
	if (!predicates.empty()) {
		sql += " WHERE " + StringUtil::Join(predicates, " AND ");
	}
	auto fragment = CreateFragment(binder, std::move(sql), std::move(types), std::move(names));
	for (idx_t idx = 0; idx < bindings.size(); ++idx) {
		replacements.emplace_back(bindings[idx], ColumnBinding(fragment->table_index, idx));
	}
	return std::move(fragment);
}

idx_t CountScans(LogicalOperator &op) {
	idx_t count = op.type == LogicalOperatorType::LOGICAL_GET ? 1 : 0;
	for (auto &child : op.children) {
		count += CountScans(*child);
	}
	return count;
}

void OptimizeWorkerFragments(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!input.context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME) ||
	    CountScans(*plan) != 1) {
		return;
	}
	auto &binder = input.optimizer.binder;
	vector<ReplacementBinding> replacements;
	reference<unique_ptr<LogicalOperator>> node = plan;
	while (true) {
		auto &op = *node.get();
		unique_ptr<LogicalOperator> fragment;
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			fragment = TryCreateAggregateFragment(binder, op.Cast<LogicalAggregate>(), replacements);
		} else if (op.type == LogicalOperatorType::LOGICAL_GET) {
			fragment = TryCreateScanFragment(binder, op.Cast<LogicalGet>(), replacements);
		}
		if (fragment) {
			node.get() = std::move(fragment);
			break;
		}
		// Below a limit only a few rows may be needed, which the driver reads faster than workers transfer the table.
		if (op.type == LogicalOperatorType::LOGICAL_LIMIT || op.children.size() != 1) {
			return;
		}
		node = op.children[0];
	}
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings = std::move(replacements);
	replacer.VisitOperator(*plan);
}

} // namespace

WorkerFragmentState::WorkerFragmentState(DistributedExecutor &executor_p, Connection &connection_p)
    : executor(executor_p), connection(connection_p) {
}

vector<unique_ptr<DataChunk>> WorkerFragmentState::Execute(ClientContext &context, const string &sql,
                                                           const vector<LogicalType> &types) {
	QueryExecutionInfo info;
	info.sql = sql;
	const auto start = std::chrono::steady_clock::now();
	auto distributed = executor.ExecuteDistributed(sql);
	const bool ran_distributed = distributed.result != nullptr || distributed.arrow_schema != nullptr;
	vector<unique_ptr<DataChunk>> chunks;
	if (distributed.arrow_schema) {
		for (auto &batch : distributed.arrow_batches) {
			auto chunk = make_uniq<DataChunk>();
			ArrowRecordBatchToDataChunk(context, *batch, *chunk, &types);
			chunks.emplace_back(std::move(chunk));
		}
	} else {
		auto result = ran_distributed ? std::move(distributed.result) : connection.Query(sql);
		if (result->HasError()) {
			result->ThrowError();
		}
		if (result->types != types) {
			throw InternalException("Worker fragment '%s' returned unexpected types", sql);
		}
		for (auto chunk = result->Fetch(); chunk && chunk->size() > 0; chunk = result->Fetch()) {
			chunks.emplace_back(std::move(chunk));
		}
	}
	if (ran_distributed) {
		info.execution_mode = distributed.partition_strategy == PartitionStrategy::NONE
		                          ? QueryExecutionMode::DELEGATED
		                          : QueryExecutionMode::ROW_GROUP_PARTITION;
		info.merge_strategy = distributed.merge_strategy;
		info.num_workers_used = distributed.num_workers_used;
		info.num_tasks_generated = distributed.num_tasks;
	}
	info.query_duration =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
	executions.emplace_back(std::move(info));
	return chunks;
}

vector<QueryExecutionInfo> WorkerFragmentState::TakeExecutions() {
	auto result = std::move(executions);
	executions.clear();
	return result;
}

OptimizerExtension GetWorkerFragmentExtension() {
	OptimizerExtension extension;
	extension.optimize_function = OptimizeWorkerFragments;
	return extension;
}

} // namespace duckdb
