#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

// Bind data for distributed table function
struct DistributedTableBindData : public TableFunctionData {
	string server_url;
	string remote_table_name;
	vector<LogicalType> column_types;
	vector<string> column_names;

	DistributedTableBindData(string server_url, string remote_table_name, vector<LogicalType> column_types,
	                         vector<string> column_names)
	    : server_url(std::move(server_url)), remote_table_name(std::move(remote_table_name)),
	      column_types(std::move(column_types)), column_names(std::move(column_names)) {
	}
};

// Global state for distributed table function
struct DistributedTableGlobalState : public GlobalTableFunctionState {
	DistributedTableGlobalState() = default;

	bool finished = false;
};

// Local state for distributed table function
struct DistributedTableLocalState : public LocalTableFunctionState {
	DistributedTableLocalState() = default;

	bool finished = false;
};

// The distributed table function
class DistributedTableFunction {
public:
	static TableFunction GetFunction();

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);

	static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state);

	static void Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	static double Progress(ClientContext &context, const FunctionData *bind_data,
	                       const GlobalTableFunctionState *global_state);
};

} // namespace duckdb
