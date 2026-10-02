#include "client/execution/distributed_table_scan_function.hpp"

#include "arrow_utils.hpp"
#include "client/duckherder_catalog.hpp"
#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "utils/catalog_utils.hpp"
#include "utils/sql_render_utils.hpp"

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
	serializer.WritePropertyWithDefault(105, "pushed_query", data.pushed_query);
	serializer.WritePropertyWithDefault(106, "pushed_types", data.pushed_types);
}

unique_ptr<FunctionData> DeserializeDistributedTableScan(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto server_url = deserializer.ReadProperty<string>(103, "server_url");
	auto remote_table_name = deserializer.ReadProperty<string>(104, "remote_table_name");
	auto &table_entry =
	    Catalog::GetEntry<TableCatalogEntry>(deserializer.Get<ClientContext &>(), catalog, schema, table);
	auto result =
	    make_uniq<DistributedTableScanBindData>(table_entry, std::move(server_url), std::move(remote_table_name));
	result->pushed_query = deserializer.ReadPropertyWithDefault<string>(105, "pushed_query");
	result->pushed_types = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(106, "pushed_types");
	return std::move(result);
}

// Without an estimate, every remote table looks like one row, so joins may build hash tables on the larger side.
unique_ptr<NodeStatistics> DistributedTableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<DistributedTableScanBindData>();
	// A pushed-down query returns aggregated rows, not table rows.
	if (!bind_data.pushed_query.empty()) {
		return nullptr;
	}
	auto &table = bind_data.table;
	auto &catalog = table.schema.catalog.Cast<DuckherderCatalog>();
	auto estimate = catalog.GetEstimatedCardinality(table.schema.name, table.name);
	if (!estimate.IsValid()) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(estimate.GetIndex(), estimate.GetIndex());
}

virtual_column_map_t GetDistributedTableScanVirtualColumns(ClientContext &context,
                                                           optional_ptr<FunctionData> bind_data) {
	return bind_data->Cast<DistributedTableScanBindData>().table.GetVirtualColumns();
}

bool DistributedTableScanSupportsPushdownType(const FunctionData &bind_data_p, idx_t column_id) {
	LogicalType type;
	GetColumnSQL(bind_data_p.Cast<DistributedTableScanBindData>().table, column_id, type);
	return SupportsRemoteFilterPushdown(type);
}

// Builds a remote query returning exactly the requested columns in output order, with pushed-down filters applied.
// Filter keys index into `filter_column_ids`, which may include columns that are not returned.
string BuildScanSQL(const DistributedTableScanBindData &bind_data, const vector<column_t> &column_ids,
                    const vector<column_t> &filter_column_ids, optional_ptr<TableFilterSet> filters,
                    vector<LogicalType> &types) {
	vector<string> select_list;
	for (auto column_id : column_ids) {
		if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			// No column is referenced (e.g. count(*)), so only the row count matters.
			select_list.emplace_back("NULL::BOOLEAN");
			types.emplace_back(LogicalType::BOOLEAN);
			continue;
		}
		LogicalType type;
		select_list.emplace_back(GetColumnSQL(bind_data.table, column_id, type));
		types.emplace_back(std::move(type));
	}
	vector<string> predicates;
	if (filters != nullptr) {
		for (auto &entry : filters->filters) {
			LogicalType type;
			auto column = GetColumnSQL(bind_data.table, filter_column_ids[entry.first], type);
			// Only join filters reach here for unsupported types; the join re-checks those rows anyway.
			if (!SupportsRemoteFilterPushdown(type)) {
				continue;
			}
			auto predicate = RemoteFilterToSQL(*entry.second, column);
			if (!predicate.empty()) {
				predicates.emplace_back(std::move(predicate));
			}
		}
	}
	return RenderSelectQuery(select_list, bind_data.remote_table_name, predicates);
}

} // namespace

struct DistributedTableScanGlobalState : public GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return MaxValue<idx_t>(batches.size(), 1);
	}

	vector<column_t> column_ids;
	// Expected types come from the table schema to handle special types like ENUM.
	vector<LogicalType> expected_types;
	// The whole remote scan result, fetched once. Each batch is converted by the thread that claims it.
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	atomic<idx_t> next_batch {0};
};

struct DistributedTableScanLocalState : public LocalTableFunctionState {
	// The claimed batch, converted to DuckDB vectors and emitted one vector at a time.
	unique_ptr<DataChunk> batch;
	idx_t batch_index = 0;
	idx_t batch_offset = 0;
};

namespace {

// Batch indexes let DuckDB preserve the remote result order, e.g. of a pushed-down ORDER BY, across threads.
OperatorPartitionData DistributedTableScanGetPartitionData(ClientContext &context,
                                                           TableFunctionGetPartitionInput &input) {
	return OperatorPartitionData(input.local_state->Cast<DistributedTableScanLocalState>().batch_index);
}

} // namespace

unique_ptr<FunctionData> DistributedTableScanBindData::Copy() const {
	auto result = make_uniq<DistributedTableScanBindData>(table, server_url, remote_table_name);
	result->pushed_query = pushed_query;
	result->pushed_types = pushed_types;
	return std::move(result);
}

bool DistributedTableScanBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<DistributedTableScanBindData>();
	return &other.table == &table && other.server_url == server_url && other.remote_table_name == remote_table_name &&
	       other.pushed_query == pushed_query && other.pushed_types == pushed_types;
}

TableFunction DistributedTableScanFunction::GetFunction() {
	TableFunction function("distributed_scan", {}, Execute, Bind, InitGlobal, InitLocal);
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.filter_prune = true;
	function.supports_pushdown_type = DistributedTableScanSupportsPushdownType;
	function.cardinality = DistributedTableScanCardinality;
	function.get_bind_info = GetBindInfo;
	function.serialize = SerializeDistributedTableScan;
	function.deserialize = DeserializeDistributedTableScan;
	function.get_virtual_columns = GetDistributedTableScanVirtualColumns;
	function.get_partition_data = DistributedTableScanGetPartitionData;
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
	auto &bind_data = input.bind_data->Cast<DistributedTableScanBindData>();
	auto global_state = make_uniq<DistributedTableScanGlobalState>();
	string scan_sql;
	if (!bind_data.pushed_query.empty()) {
		global_state->column_ids = input.column_ids;
		scan_sql = bind_data.pushed_query;
		global_state->expected_types = bind_data.pushed_types;
	} else {
		if (!input.projection_ids.empty()) {
			// With filter pruning, the output follows `projection_ids`, which may reorder or drop filter-only columns.
			for (auto projection_id : input.projection_ids) {
				global_state->column_ids.emplace_back(input.column_ids[projection_id]);
			}
		} else {
			global_state->column_ids = input.column_ids;
		}
		if (global_state->column_ids.empty()) {
			for (idx_t col_idx = 0; col_idx < bind_data.table.GetColumns().LogicalColumnCount(); ++col_idx) {
				global_state->column_ids.emplace_back(col_idx);
			}
		}
		// Filters include join filters pushed from the build side, which are only known once the scan starts.
		scan_sql = BuildScanSQL(bind_data, global_state->column_ids, input.column_ids, input.filters,
		                        global_state->expected_types);
	}

	auto &client = GetDistributedClient(context, bind_data.table);
	auto status = client.ScanTableBatches(scan_sql, global_state->batches);
	if (!status.ok()) {
		throw IOException("Distributed table scan error: %s", status.ToString());
	}
	return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> DistributedTableScanFunction::InitLocal(ExecutionContext &context,
                                                                            TableFunctionInitInput &input,
                                                                            GlobalTableFunctionState *global_state) {
	return make_uniq<DistributedTableScanLocalState>();
}

void DistributedTableScanFunction::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<DistributedTableScanGlobalState>();
	auto &local_state = data.local_state->Cast<DistributedTableScanLocalState>();

	while (!local_state.batch || local_state.batch_offset == local_state.batch->size()) {
		auto batch_idx = global_state.next_batch++;
		if (batch_idx >= global_state.batches.size()) {
			output.SetCardinality(0);
			return;
		}
		// Only this thread claims the batch, so it can release the Arrow copy once converted.
		auto batch = std::move(global_state.batches[batch_idx]);
		local_state.batch = make_uniq<DataChunk>();
		ArrowRecordBatchToDataChunk(context, *batch, *local_state.batch, &global_state.expected_types);
		local_state.batch_index = batch_idx;
		local_state.batch_offset = 0;
	}

	// The remote query already returns the projected columns in output order.
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.batch->size() - local_state.batch_offset);
	output.SetCardinality(count);
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); ++col_idx) {
		if (global_state.column_ids[col_idx] == COLUMN_IDENTIFIER_EMPTY) {
			continue;
		}
		VectorOperations::Copy(local_state.batch->data[col_idx], output.data[col_idx], local_state.batch_offset + count,
		                       local_state.batch_offset, /*target_offset=*/0);
	}
	local_state.batch_offset += count;
}

} // namespace duckdb
