#include "client/execution/distributed_table_scan_function.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

void SerializeDistributedTableScan(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                   const TableFunction &function) {
	auto &data = bind_data->Cast<DistributedTableScanBindData>();
	serializer.WriteProperty(100, "catalog", data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", data.table.schema.name);
	serializer.WriteProperty(102, "table", data.table.name);
	serializer.WriteProperty(103, "server_url", data.server_url);
	serializer.WriteProperty(104, "remote_table_name", data.remote_table_name);
}

unique_ptr<FunctionData> DeserializeDistributedTableScan(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto server_url = deserializer.ReadProperty<string>(103, "server_url");
	auto remote_table_name = deserializer.ReadProperty<string>(104, "remote_table_name");
	auto &table_entry =
	    Catalog::GetEntry<TableCatalogEntry>(deserializer.Get<ClientContext &>(), catalog, schema, table);
	return make_uniq<DistributedTableScanBindData>(table_entry, std::move(server_url), std::move(remote_table_name));
}

virtual_column_map_t GetDistributedTableScanVirtualColumns(ClientContext &context,
                                                           optional_ptr<FunctionData> bind_data) {
	return bind_data->Cast<DistributedTableScanBindData>().table.GetVirtualColumns();
}

} // namespace

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
	function.get_bind_info = GetBindInfo;
	function.serialize = SerializeDistributedTableScan;
	function.deserialize = DeserializeDistributedTableScan;
	function.get_virtual_columns = GetDistributedTableScanVirtualColumns;
	return function;
}

BindInfo DistributedTableScanFunction::GetBindInfo(optional_ptr<FunctionData> bind_data) {
	return BindInfo(bind_data->Cast<DistributedTableScanBindData>().table);
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

	// Get the expected types from the table schema to handle special types like ENUM.
	auto expected_types = bind_data.table.GetColumns().GetColumnTypes();
	auto includes_rowid =
	    std::find(local_state.column_ids.begin(), local_state.column_ids.end(), COLUMN_IDENTIFIER_ROW_ID) !=
	    local_state.column_ids.end();
	auto scan_source = bind_data.remote_table_name;
	if (includes_rowid) {
		expected_types.insert(expected_types.begin(), LogicalType::ROW_TYPE);
		scan_source = StringUtil::Format("SELECT rowid, * FROM %s", bind_data.remote_table_name);
	}
	auto &client = GetDistributedClient(context, bind_data.table);
	auto result =
	    client.ScanTable(scan_source, /*limit=*/output.GetCapacity(), local_state.offset, &expected_types);
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
			auto source_idx = col_idx == COLUMN_IDENTIFIER_ROW_ID ? 0 : col_idx + (includes_rowid ? 1 : 0);
			if (source_idx < data_chunk->ColumnCount()) {
				VectorOperations::Copy(data_chunk->data[source_idx], output.data[out_idx], data_chunk->size(),
				                       /*source_offset=*/0, /*target_offset=*/0);
			}
		}
	}
	local_state.offset += data_chunk->size();
}

} // namespace duckdb
