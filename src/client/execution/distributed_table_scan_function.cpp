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
#include "duckdb/parser/keyword_helper.hpp"
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

// Builds a remote query returning exactly the requested columns, in output order.
string BuildProjectedScanSQL(const DistributedTableScanBindData &bind_data, const vector<column_t> &column_ids,
                             vector<LogicalType> &types) {
	const auto virtual_columns = bind_data.table.GetVirtualColumns();
	vector<string> select_list;
	for (auto column_id : column_ids) {
		if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			// No column is referenced (e.g. count(*)), so only the row count matters.
			select_list.emplace_back("NULL::BOOLEAN");
			types.emplace_back(LogicalType::BOOLEAN);
		} else if (IsVirtualColumn(column_id)) {
			auto entry = virtual_columns.find(column_id);
			if (entry == virtual_columns.end()) {
				throw InternalException("Distributed table scan received unregistered virtual column %llu", column_id);
			}
			select_list.emplace_back(KeywordHelper::WriteOptionallyQuoted(entry->second.name));
			types.emplace_back(entry->second.type);
		} else {
			auto &column = bind_data.table.GetColumn(LogicalIndex(column_id));
			select_list.emplace_back(KeywordHelper::WriteOptionallyQuoted(column.Name()));
			types.emplace_back(column.Type());
		}
	}
	return StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), bind_data.remote_table_name);
}

} // namespace

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
	// The whole remote scan result, fetched once and drained one chunk per Execute call.
	unique_ptr<QueryResult> result;
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
	if (local_state->column_ids.empty()) {
		auto &bind_data = input.bind_data->Cast<DistributedTableScanBindData>();
		for (idx_t col_idx = 0; col_idx < bind_data.table.GetColumns().LogicalColumnCount(); ++col_idx) {
			local_state->column_ids.emplace_back(col_idx);
		}
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

	if (!local_state.result) {
		// Expected types come from the table schema to handle special types like ENUM.
		vector<LogicalType> expected_types;
		auto scan_source = BuildProjectedScanSQL(bind_data, local_state.column_ids, expected_types);
		// Paging with LIMIT/OFFSET re-reads the remaining table per chunk and has no stable row order.
		auto &client = GetDistributedClient(context, bind_data.table);
		local_state.result = client.ScanTable(scan_source, NO_QUERY_LIMIT, NO_QUERY_OFFSET, &expected_types);
		if (local_state.result->HasError()) {
			throw Exception(ExceptionType::INTERNAL,
			                StringUtil::Format("Distributed table scan error: %s", local_state.result->GetError()));
		}
	}

	auto data_chunk = local_state.result->Fetch();

	// No more data, and mark as finished.
	if (data_chunk == nullptr || data_chunk->size() == 0) {
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	// The remote query already returns the projected columns in output order.
	output.SetCardinality(data_chunk->size());
	for (idx_t col_idx = 0; col_idx < output.ColumnCount() && col_idx < data_chunk->ColumnCount(); ++col_idx) {
		if (local_state.column_ids[col_idx] == COLUMN_IDENTIFIER_EMPTY) {
			continue;
		}
		VectorOperations::Copy(data_chunk->data[col_idx], output.data[col_idx], data_chunk->size(),
		                       /*source_offset=*/0, /*target_offset=*/0);
	}
}

} // namespace duckdb
