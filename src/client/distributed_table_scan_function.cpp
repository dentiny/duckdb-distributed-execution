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
	idx_t offset;  // Track current offset for fetching data
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

	auto &db = DatabaseInstance::GetDatabase(context);
	DUCKDB_LOG_DEBUG(db, StringUtil::Format("Distributed scan executing for table: %s from server: %s",
	                                        bind_data.remote_table_name, bind_data.server_url));

	if (local_state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &client = DistributedClient::GetInstance();
	if (!client.TableExists(bind_data.remote_table_name)) {
		DUCKDB_LOG_DEBUG(db, StringUtil::Format("Table %s does not exist on server", bind_data.remote_table_name));
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	DUCKDB_LOG_DEBUG(db, StringUtil::Format("Fetching data from server for table: %s (offset: %llu)", 
	                                        bind_data.remote_table_name, 
	                                        static_cast<long long unsigned>(local_state.offset)));
	// Get the expected types from the table schema to handle special types like ENUM
	auto expected_types = bind_data.table.GetColumns().GetColumnTypes();
	auto result =
	    client.ScanTable(bind_data.remote_table_name, /*limit=*/output.GetCapacity(), /*offset=*/local_state.offset, &expected_types);

	if (result->HasError()) {
		throw Exception(ExceptionType::INTERNAL, "Distributed table scan error: " + result->GetError());
	}

	auto data_chunk = result->Fetch();
	if (data_chunk && data_chunk->size() > 0) {
		// Handle projection pushdown: copy data from fetched chunk to output
		// Note: We use Copy instead of Reference to handle column reordering correctly.
		// The output DataChunk schema is determined by the query projection,
		// while data_chunk has the table's natural column order.
		
		output.SetCardinality(data_chunk->size());
		
		if (local_state.column_ids.empty()) {
			// No projection - copy all columns in order
			for (idx_t col_idx = 0; col_idx < std::min(output.ColumnCount(), data_chunk->ColumnCount()); col_idx++) {
				VectorOperations::Copy(data_chunk->data[col_idx], output.data[col_idx], data_chunk->size(), 0, 0);
			}
		} else {
			// Projection pushdown - copy only requested columns in correct order
			for (idx_t out_idx = 0; out_idx < output.ColumnCount() && out_idx < local_state.column_ids.size(); out_idx++) {
				auto col_idx = local_state.column_ids[out_idx];
				if (col_idx < data_chunk->ColumnCount()) {
					VectorOperations::Copy(data_chunk->data[col_idx], output.data[out_idx], data_chunk->size(), 0, 0);
				}
			}
		}

		// FIX: Increment offset to fetch next chunk on next call
		// Don't mark as finished yet - keep fetching until we get an empty chunk
		local_state.offset += data_chunk->size();
		
		DUCKDB_LOG_DEBUG(db, StringUtil::Format("Fetched %llu rows, new offset: %llu", 
		                                        static_cast<long long unsigned>(data_chunk->size()),
		                                        static_cast<long long unsigned>(local_state.offset)));
	} else {
		// No more data - mark as finished
		output.SetCardinality(0);
		local_state.finished = true;
		DUCKDB_LOG_DEBUG(db, StringUtil::Format("Scan finished for table: %s (total rows: %llu)", 
		                                        bind_data.remote_table_name,
		                                        static_cast<long long unsigned>(local_state.offset)));
	}
}

} // namespace duckdb
