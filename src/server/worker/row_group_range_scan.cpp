#include "server/worker/row_group_range_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

namespace {

struct RowGroupRangeScanGlobalState : public GlobalTableFunctionState {
	explicit RowGroupRangeScanGlobalState(DuckTransaction &transaction_p) : transaction(transaction_p) {
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

	DuckTransaction &transaction;
	ParallelTableScanState state;
	idx_t max_threads = 0;
	// Set if filter-only columns are removed from the output.
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
};

struct RowGroupRangeScanLocalState : public LocalTableFunctionState {
	TableScanState scan_state;
	DataChunk all_columns;
};

optional_ptr<TableFilter> GetRowIdFilter(const TableFilterSet &filters, const vector<ColumnIndex> &column_indexes) {
	for (auto &entry : filters.filters) {
		if (column_indexes[entry.first].IsRowIdColumn()) {
			return entry.second.get();
		}
	}
	return nullptr;
}

DuckTableEntry &GetTable(const FunctionData &bind_data) {
	return bind_data.Cast<TableScanBindData>().table.Cast<DuckTableEntry>();
}

unique_ptr<GlobalTableFunctionState> RowGroupRangeScanInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &table = GetTable(*input.bind_data);
	auto result = make_uniq<RowGroupRangeScanGlobalState>(DuckTransaction::Get(context, table.catalog));
	table.GetStorage().InitializeParallelScan(context, result->state, input.column_indexes);

	// Starts at the first row group the filter can match and ends after the last one.
	auto &filter = *GetRowIdFilter(*input.filters, input.column_indexes);
	auto &scan_state = result->state.scan_state;
	auto can_match = [&](SegmentNode<RowGroup> &row_group) {
		return RowGroup::CheckRowIdFilter(filter, row_group.GetRowStart(), row_group.GetRowEnd()) !=
		       FilterPropagateResult::FILTER_ALWAYS_FALSE;
	};
	while (scan_state.current_row_group && !can_match(*scan_state.current_row_group)) {
		scan_state.current_row_group =
		    scan_state.GetNextRowGroup(*scan_state.row_groups, *scan_state.current_row_group);
	}
	idx_t max_row = 0;
	for (auto row_group = scan_state.current_row_group; row_group && can_match(*row_group);
	     row_group = scan_state.GetNextRowGroup(*scan_state.row_groups, *row_group)) {
		max_row = row_group->GetRowEnd();
		++result->max_threads;
	}
	scan_state.max_row = MinValue(scan_state.max_row, max_row);

	if (input.CanRemoveFilterColumns()) {
		result->projection_ids = input.projection_ids;
		for (auto &column : input.column_indexes) {
			if (column.IsRowIdColumn()) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else if (column.HasType()) {
				result->scanned_types.emplace_back(column.GetScanType());
			} else {
				result->scanned_types.emplace_back(table.GetColumns().GetColumn(column.ToLogical()).Type());
			}
		}
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> RowGroupRangeScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	auto &state = global_state->Cast<RowGroupRangeScanGlobalState>();
	auto &table = GetTable(*input.bind_data);
	auto result = make_uniq<RowGroupRangeScanLocalState>();
	vector<StorageIndex> storage_ids;
	for (auto &column : input.column_indexes) {
		storage_ids.emplace_back(table.GetStorageIndex(column));
	}
	result->scan_state.Initialize(std::move(storage_ids), context.client, input.filters, input.sample_options);
	table.GetStorage().NextParallelScan(context.client, state.state, result->scan_state);
	if (!state.projection_ids.empty()) {
		result->all_columns.Initialize(context.client, state.scanned_types);
	}
	return std::move(result);
}

void RowGroupRangeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RowGroupRangeScanGlobalState>();
	auto &local_state = data.local_state->Cast<RowGroupRangeScanLocalState>();
	auto &storage = GetTable(*data.bind_data).GetStorage();
	do {
		if (state.projection_ids.empty()) {
			storage.Scan(state.transaction, output, local_state.scan_state);
		} else {
			local_state.all_columns.Reset();
			storage.Scan(state.transaction, local_state.all_columns, local_state.scan_state);
			output.ReferenceColumns(local_state.all_columns, state.projection_ids);
		}
		if (output.size() > 0) {
			return;
		}
	} while (storage.NextParallelScan(context, state.state, local_state.scan_state) > 0);
}

OperatorPartitionData RowGroupRangeScanGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	return OperatorPartitionData(
	    input.local_state->Cast<RowGroupRangeScanLocalState>().scan_state.table_state.batch_index);
}

void ReplaceRowIdRangeScans(LogicalOperator &op) {
	for (auto &child : op.children) {
		ReplaceRowIdRangeScans(*child);
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op.Cast<LogicalGet>();
	// Row group ordering, as used for top-N, is only implemented by the table scan.
	// Unlike in the physical plan, logical table filters are keyed by table column.
	if (get.function.name != "seq_scan" || get.bind_data->Cast<TableScanBindData>().order_options ||
	    get.table_filters.filters.find(COLUMN_IDENTIFIER_ROW_ID) == get.table_filters.filters.end()) {
		return;
	}
	get.function.function = RowGroupRangeScan;
	get.function.init_global = RowGroupRangeScanInitGlobal;
	get.function.init_local = RowGroupRangeScanInitLocal;
	get.function.get_partition_data = RowGroupRangeScanGetPartitionData;
	// These read the table scan's own states.
	get.function.table_scan_progress = nullptr;
	get.function.get_metrics = nullptr;
}

void OptimizeRowGroupRangeScans(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	ReplaceRowIdRangeScans(*plan);
}

} // namespace

OptimizerExtension GetRowGroupRangeScanExtension() {
	OptimizerExtension extension;
	extension.optimize_function = OptimizeRowGroupRangeScans;
	return extension;
}

} // namespace duckdb
