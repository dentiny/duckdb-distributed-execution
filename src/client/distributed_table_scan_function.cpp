#include "distributed_table_scan_function.hpp"

#include "distributed_client.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"

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
	return function;
}

unique_ptr<FunctionData> DistributedTableScanFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
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
	return std::move(local_state);
}

void DistributedTableScanFunction::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<DistributedTableScanBindData>();
	auto &local_state = data.local_state->Cast<DistributedTableScanLocalState>();

	if (local_state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &client = DistributedClient::GetInstance();
	if (!client.TableExists(bind_data.remote_table_name)) {
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	// Get the expected types from the table schema to handle special types like ENUM.
	auto expected_types = bind_data.table.GetColumns().GetColumnTypes();
	auto result = client.ScanTable(bind_data.remote_table_name, /*limit=*/output.GetCapacity(), local_state.offset,
	                               &expected_types);
	if (result->HasError()) {
		throw Exception(ExceptionType::INTERNAL,
		                StringUtil::Format("Distributed table scan error: %s", result->GetError()));
	}

	auto data_chunk = result->Fetch();

	// No more data, and mark as finished.
	if (data_chunk == nullptr || data_chunk->size() == 0) {
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	// Handle projection pushdown: copy data from fetched chunk to output.
	// Note: We use Copy instead of Reference to handle column reordering correctly.
	// The output DataChunk schema is determined by the query projection, while data_chunk has the table's natural
	// column order.
	output.SetCardinality(data_chunk->size());

	// If there's no projection, just copy all columns in order.
	if (local_state.column_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < std::min(output.ColumnCount(), data_chunk->ColumnCount()); ++col_idx) {
			VectorOperations::Copy(data_chunk->data[col_idx], output.data[col_idx], data_chunk->size(),
			                       /*source_offset=*/0, /*target_offset=*/0);
		}
	}
	// Otherwise, perform projection pushdown, and copy only requested columns in the correct order.
	else {
		for (idx_t out_idx = 0; out_idx < output.ColumnCount() && out_idx < local_state.column_ids.size(); ++out_idx) {
			auto col_idx = local_state.column_ids[out_idx];
			if (col_idx < data_chunk->ColumnCount()) {
				VectorOperations::Copy(data_chunk->data[col_idx], output.data[out_idx], data_chunk->size(),
				                       /*source_offset=*/0, /*target_offset=*/0);
			}
		}
	}
	local_state.offset += data_chunk->size();
}

} // namespace duckdb
