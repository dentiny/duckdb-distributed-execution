#include "distributed_table_scan_function.hpp"

#include "distributed_client.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckherder_transaction_manager.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

struct DistributedTableScanGlobalState : public GlobalTableFunctionState {
	DistributedTableScanGlobalState() : finished(false) {
	}
	bool finished;
};

struct DistributedTableScanLocalState : public LocalTableFunctionState {
	DistributedTableScanLocalState() : finished(false), offset(0) {
	}
	bool finished;
	vector<column_t> column_ids;
	idx_t offset; // Track current offset for fetching data
	ScanTableOptions options;
	vector<LogicalType> expected_types;
	vector<idx_t> output_to_remote;
};

unique_ptr<FunctionData> DistributedTableScanBindData::Copy() const {
	return make_uniq<DistributedTableScanBindData>(table, server_url, remote_table_name);
}

bool DistributedTableScanBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<DistributedTableScanBindData>();
	return &other.table == &table && other.server_url == server_url && other.remote_table_name == remote_table_name;
}

TableFunction DistributedTableScanFunction::GetFunction() {
	TableFunction function("distributed_scan", {}, Execute, Bind, InitGlobal, InitLocal);
	function.projection_pushdown = true;
	function.filter_pushdown = false;
	function.get_bind_info = GetBindInfo;
	return function;
}

BindInfo DistributedTableScanFunction::GetBindInfo(optional_ptr<FunctionData> bind_data) {
	return BindInfo(bind_data->Cast<DistributedTableScanBindData>().table);
}

unique_ptr<FunctionData> DistributedTableScanFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
	throw Exception(ExceptionType::INTERNAL, "DistributedTableScanFunction::Bind should not be called directly");
}

unique_ptr<GlobalTableFunctionState> DistributedTableScanFunction::InitGlobal(ClientContext &context,
                                                                              TableFunctionInitInput &input) {
	return make_uniq<DistributedTableScanGlobalState>();
}

unique_ptr<LocalTableFunctionState> DistributedTableScanFunction::InitLocal(ExecutionContext &context,
                                                                            TableFunctionInitInput &input,
                                                                            GlobalTableFunctionState *global_state) {
	auto local_state = make_uniq<DistributedTableScanLocalState>();
	local_state->column_ids = input.column_ids;
	auto &bind_data = input.bind_data->Cast<DistributedTableScanBindData>();
	auto &transaction_manager =
	    bind_data.table.ParentCatalog().GetAttached().GetTransactionManager().Cast<DuckherderTransactionManager>();
	local_state->options.session_id = transaction_manager.GetRemoteSessionForScan(context.client);

	bool can_project = !local_state->column_ids.empty();
	for (auto column_id : local_state->column_ids) {
		if (column_id != COLUMN_IDENTIFIER_ROW_ID && column_id >= bind_data.table.GetColumns().LogicalColumnCount()) {
			can_project = false;
			break;
		}
		local_state->options.include_rowid |= column_id == COLUMN_IDENTIFIER_ROW_ID;
	}
	local_state->options.project_columns = can_project;

	if (can_project) {
		idx_t next_remote_column = local_state->options.include_rowid ? 1 : 0;
		if (local_state->options.include_rowid) {
			local_state->expected_types.push_back(LogicalType::BIGINT);
		}
		for (auto column_id : local_state->column_ids) {
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				local_state->output_to_remote.push_back(0);
				continue;
			}
			auto &column = bind_data.table.GetColumns().GetColumn(LogicalIndex(column_id));
			local_state->options.projected_columns.push_back(column.Name());
			local_state->expected_types.push_back(column.Type());
			local_state->output_to_remote.push_back(next_remote_column++);
		}
	} else {
		local_state->expected_types = bind_data.table.GetColumns().GetColumnTypes();
	}
	return std::move(local_state);
}

void DistributedTableScanFunction::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<DistributedTableScanBindData>();
	auto &local_state = data.local_state->Cast<DistributedTableScanLocalState>();

	if (local_state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &client = GetDistributedClient(bind_data.table);
	auto result = client.ScanTable(bind_data.remote_table_name, /*limit=*/output.GetCapacity(),
	                               local_state.offset, &local_state.expected_types, local_state.options);
	if (result->HasError()) {
		throw IOException("Distributed table scan error: %s", result->GetError());
	}

	auto data_chunk = result->Fetch();

	// No more data, and mark as finished.
	if (data_chunk == nullptr || data_chunk->size() == 0) {
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	// Handle projection pushdown by referencing the matching fetched vectors. Referencing preserves vector validity
	// and auxiliary buffers while still allowing projected columns to be reordered.
	output.SetCardinality(data_chunk->size());

	// If there's no projection, reference all columns in order.
	if (local_state.column_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < std::min(output.ColumnCount(), data_chunk->ColumnCount()); ++col_idx) {
			output.data[col_idx].Reference(data_chunk->data[col_idx]);
		}
	}
	// Otherwise, reference only requested columns in the correct order.
	else if (local_state.options.project_columns) {
		for (idx_t out_idx = 0; out_idx < output.ColumnCount() && out_idx < local_state.output_to_remote.size();
		     ++out_idx) {
			auto remote_idx = local_state.output_to_remote[out_idx];
			if (remote_idx < data_chunk->ColumnCount()) {
				output.data[out_idx].Reference(data_chunk->data[remote_idx]);
			}
		}
	} else {
		for (idx_t out_idx = 0; out_idx < output.ColumnCount() && out_idx < local_state.column_ids.size(); ++out_idx) {
			auto col_idx = local_state.column_ids[out_idx];
			if (col_idx < data_chunk->ColumnCount()) {
				output.data[out_idx].Reference(data_chunk->data[col_idx]);
			}
		}
	}
	local_state.offset += data_chunk->size();
}

} // namespace duckdb
