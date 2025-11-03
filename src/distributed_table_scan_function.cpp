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
	DistributedTableScanLocalState() : finished(false) {
	}
	bool finished;
	vector<column_t> column_ids;
};

unique_ptr<FunctionData> DistributedTableScanBindData::Copy() const {
	return make_uniq<DistributedTableScanBindData>(table, server_url, remote_table_name);
}

bool DistributedTableScanBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<DistributedTableScanBindData>();
	return &other.table == &table && other.server_url == server_url && other.remote_table_name == remote_table_name;
}

static BindInfo DistributedTableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<DistributedTableScanBindData>();
	return BindInfo(bind_data.table);
}

TableFunction DistributedTableScanFunction::GetFunction() {
	TableFunction function("distributed_scan", {}, Execute, Bind, InitGlobal, InitLocal);
	function.projection_pushdown = true;
	function.filter_pushdown = false;
	function.get_bind_info = DistributedTableScanGetBindInfo;
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

	// TODO(hjiang): Currently fake the interaction with server, should replace with client impl.
	auto &client = DistributedClient::GetInstance();
	if (!client.TableExists(bind_data.remote_table_name)) {
		DUCKDB_LOG_DEBUG(db, StringUtil::Format("Table %s does not exist on server", bind_data.remote_table_name));
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	DUCKDB_LOG_DEBUG(db, StringUtil::Format("Fetching data from server for table: %s", bind_data.remote_table_name));
	auto result = client.ScanTable(bind_data.remote_table_name, output.GetCapacity(), 0);

	if (result->HasError()) {
		throw Exception(ExceptionType::INTERNAL, "Distributed table scan error: " + result->GetError());
	}

	auto data_chunk = result->Fetch();
	if (data_chunk && data_chunk->size() > 0) {
		// Handle projection pushdown: output may have fewer columns than the fetched data
		// We need to only reference the columns that are requested
		if (local_state.column_ids.empty() || output.ColumnCount() == data_chunk->ColumnCount()) {
			// No projection or all columns requested
			output.Reference(*data_chunk);
		} else {
			// Projection pushdown: only reference the requested columns
			for (idx_t out_idx = 0; out_idx < output.ColumnCount(); out_idx++) {
				auto col_idx = local_state.column_ids[out_idx];
				if (col_idx < data_chunk->ColumnCount()) {
					output.data[out_idx].Reference(data_chunk->data[col_idx]);
				}
			}
			output.SetCardinality(data_chunk->size());
		}

		// TODO(hjiang): For simplicity, we only return one chunk.
		local_state.finished = true;
	} else {
		output.SetCardinality(0);
		local_state.finished = true;
	}
}

} // namespace duckdb
