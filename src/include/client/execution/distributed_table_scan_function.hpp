#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Forward declaration.
class TableCatalogEntry;

struct DistributedTableScanBindData : public TableFunctionData {
	explicit DistributedTableScanBindData(TableCatalogEntry &table_p, string server_url_p, string remote_table_name_p)
	    : table(table_p), server_url(std::move(server_url_p)), remote_table_name(std::move(remote_table_name_p)) {
	}

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;

	TableCatalogEntry &table;
	string server_url;
	string remote_table_name;
	// Set when an operator above the scan, e.g. an aggregate, was pushed into the remote query.
	// The scan then returns `pushed_types` from `pushed_query` instead of table columns.
	string pushed_query;
	vector<LogicalType> pushed_types;
};

// Returns the quoted remote name and type of a physical or virtual table column.
string GetRemoteColumn(const DistributedTableScanBindData &bind_data, column_t column_id, LogicalType &type);

// The distributed table scan function that replaces regular table scans.
class DistributedTableScanFunction {
public:
	static TableFunction GetFunction();

private:
	static BindInfo GetBindInfo(optional_ptr<FunctionData> bind_data);

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);

	static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state);

	static void Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

} // namespace duckdb
