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
#include "duckdb/planner/filter/list.hpp"
#include "duckdb/planner/table_filter.hpp"
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

// Returns the quoted name and type of a physical or virtual table column.
string GetScanColumn(const DistributedTableScanBindData &bind_data, const virtual_column_map_t &virtual_columns,
                     column_t column_id, LogicalType &type) {
	if (IsVirtualColumn(column_id)) {
		auto entry = virtual_columns.find(column_id);
		if (entry == virtual_columns.end()) {
			throw InternalException("Distributed table scan received unregistered virtual column %llu", column_id);
		}
		type = entry->second.type;
		return KeywordHelper::WriteOptionallyQuoted(entry->second.name);
	}
	auto &column = bind_data.table.GetColumn(LogicalIndex(column_id));
	type = column.Type();
	return KeywordHelper::WriteOptionallyQuoted(column.Name());
}

// Untyped literals may compare with different semantics on the server, e.g. a FLOAT column against a DECIMAL
// literal, or an ENUM column against a VARCHAR literal. ENUMs order by position, so they compare by code.
string FilterOperand(const string &column, const LogicalType &column_type) {
	if (column_type.id() == LogicalTypeId::ENUM) {
		return StringUtil::Format("enum_code(%s)", column);
	}
	return column;
}

// Renders `value` as a literal of the column type; fails if the value cannot be represented exactly.
bool TryFilterLiteral(const Value &value, const LogicalType &column_type, string &literal) {
	Value typed_value = value;
	if (value.type() != column_type && !typed_value.DefaultTryCastAs(column_type, /*strict=*/true)) {
		return false;
	}
	if (column_type.id() == LogicalTypeId::ENUM) {
		literal = std::to_string(EnumType::GetPos(column_type, typed_value.ToString()));
		return true;
	}
	if (column_type.IsNested()) {
		literal = typed_value.ToSQLString();
		return true;
	}
	// Rebuild the type so a user type alias, which the server may not know, is not rendered.
	auto target_type =
	    column_type.id() == LogicalTypeId::DECIMAL
	        ? LogicalType::DECIMAL(DecimalType::GetWidth(column_type), DecimalType::GetScale(column_type))
	        : LogicalType(column_type.id());
	literal = StringUtil::Format("CAST(%s AS %s)", typed_value.ToSQLString(), target_type.ToString());
	return true;
}

string UntranslatableFilter(const TableFilter &filter, const string &column, bool required) {
	if (!required) {
		return "";
	}
	throw InternalException("Distributed table scan cannot push down table filter %s", filter.ToString(column));
}

// Translates a table filter into a SQL predicate on `column`.
// Returns an empty string when the filter does not constrain the scan, which is only allowed for optional filters.
string TableFilterToSQL(const TableFilter &filter, const string &column, const LogicalType &column_type,
                        bool required) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		string literal;
		if (!TryFilterLiteral(constant_filter.constant, column_type, literal)) {
			return UntranslatableFilter(filter, column, required);
		}
		return StringUtil::Format("%s %s %s", FilterOperand(column, column_type),
		                          ExpressionTypeToOperator(constant_filter.comparison_type), literal);
	}
	case TableFilterType::IS_NULL:
		return StringUtil::Format("%s IS NULL", column);
	case TableFilterType::IS_NOT_NULL:
		return StringUtil::Format("%s IS NOT NULL", column);
	case TableFilterType::CONJUNCTION_AND: {
		vector<string> predicates;
		for (auto &child : filter.Cast<ConjunctionAndFilter>().child_filters) {
			auto predicate = TableFilterToSQL(*child, column, column_type, required);
			if (!predicate.empty()) {
				predicates.emplace_back(std::move(predicate));
			}
		}
		if (predicates.empty()) {
			return "";
		}
		return StringUtil::Format("(%s)", StringUtil::Join(predicates, " AND "));
	}
	case TableFilterType::CONJUNCTION_OR: {
		vector<string> predicates;
		for (auto &child : filter.Cast<ConjunctionOrFilter>().child_filters) {
			auto predicate = TableFilterToSQL(*child, column, column_type, required);
			// One unconstrained branch makes the whole disjunction unconstrained.
			if (predicate.empty()) {
				return "";
			}
			predicates.emplace_back(std::move(predicate));
		}
		return StringUtil::Format("(%s)", StringUtil::Join(predicates, " OR "));
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		auto child_column = struct_filter.child_name.empty()
		                        ? StringUtil::Format("struct_extract_at(%s, %llu)", column, struct_filter.child_idx + 1)
		                        : StringUtil::Format("struct_extract(%s, %s)", column,
		                                             KeywordHelper::WriteQuoted(struct_filter.child_name, '\''));
		return TableFilterToSQL(*struct_filter.child_filter, child_column,
		                        StructType::GetChildType(column_type, struct_filter.child_idx), required);
	}
	case TableFilterType::IN_FILTER: {
		vector<string> literals;
		for (auto &value : filter.Cast<InFilter>().values) {
			string literal;
			if (!TryFilterLiteral(value, column_type, literal)) {
				return UntranslatableFilter(filter, column, required);
			}
			literals.emplace_back(std::move(literal));
		}
		return StringUtil::Format("%s IN (%s)", FilterOperand(column, column_type), StringUtil::Join(literals, ", "));
	}
	case TableFilterType::OPTIONAL_FILTER:
		return TableFilterToSQL(*filter.Cast<OptionalFilter>().child_filter, column, column_type,
		                        /*required=*/false);
	case TableFilterType::DYNAMIC_FILTER: {
		auto &filter_data = *filter.Cast<DynamicFilter>().filter_data;
		lock_guard<mutex> guard(filter_data.lock);
		// An unset dynamic filter accepts every row.
		if (!filter_data.initialized || filter_data.filter == nullptr) {
			return "";
		}
		return TableFilterToSQL(*filter_data.filter, column, column_type, required);
	}
	default:
		return UntranslatableFilter(filter, column, required);
	}
}

// Builds a remote query returning exactly the requested columns in output order, with pushed-down filters applied.
// Filter keys index into `column_ids`.
string BuildScanSQL(const DistributedTableScanBindData &bind_data, const vector<column_t> &column_ids,
                    optional_ptr<TableFilterSet> filters, vector<LogicalType> &types) {
	const auto virtual_columns = bind_data.table.GetVirtualColumns();
	vector<string> select_list;
	for (auto column_id : column_ids) {
		if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			// No column is referenced (e.g. count(*)), so only the row count matters.
			select_list.emplace_back("NULL::BOOLEAN");
			types.emplace_back(LogicalType::BOOLEAN);
			continue;
		}
		LogicalType type;
		select_list.emplace_back(GetScanColumn(bind_data, virtual_columns, column_id, type));
		types.emplace_back(std::move(type));
	}
	auto sql =
	    StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), bind_data.remote_table_name);

	if (filters == nullptr) {
		return sql;
	}
	vector<string> predicates;
	for (auto &entry : filters->filters) {
		LogicalType type;
		auto column = GetScanColumn(bind_data, virtual_columns, column_ids[entry.first], type);
		auto predicate = TableFilterToSQL(*entry.second, column, type, /*required=*/true);
		if (!predicate.empty()) {
			predicates.emplace_back(std::move(predicate));
		}
	}
	if (predicates.empty()) {
		return sql;
	}
	return StringUtil::Format("%s WHERE %s", sql, StringUtil::Join(predicates, " AND "));
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
	string scan_sql;
	// Expected types come from the table schema to handle special types like ENUM.
	vector<LogicalType> expected_types;
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
	function.filter_pushdown = true;
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
	auto &bind_data = input.bind_data->Cast<DistributedTableScanBindData>();
	auto local_state = make_uniq<DistributedTableScanLocalState>();
	local_state->column_ids = input.column_ids;
	if (local_state->column_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < bind_data.table.GetColumns().LogicalColumnCount(); ++col_idx) {
			local_state->column_ids.emplace_back(col_idx);
		}
	}
	// Filters include join filters pushed from the build side, which are only known once the scan starts.
	local_state->scan_sql =
	    BuildScanSQL(bind_data, local_state->column_ids, input.filters, local_state->expected_types);
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
		// Paging with LIMIT/OFFSET re-reads the remaining table per chunk and has no stable row order.
		auto &client = GetDistributedClient(context, bind_data.table);
		local_state.result =
		    client.ScanTable(local_state.scan_sql, NO_QUERY_LIMIT, NO_QUERY_OFFSET, &local_state.expected_types);
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
