#include "distributed_table_function.hpp"

#include <iostream>

#include "distributed_server.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

TableFunction DistributedTableFunction::GetFunction() {
	// Define the function arguments: server_url (VARCHAR) and table_name (VARCHAR)
	vector<LogicalType> arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR};

	TableFunction function("distributed_table", arguments, Execute, Bind, InitGlobal, InitLocal);
	function.cardinality = [](ClientContext &context, const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
		return make_uniq<NodeStatistics>(1000); // Estimated cardinality
	};
	function.projection_pushdown = false;
	return function;
}

unique_ptr<FunctionData> DistributedTableFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Extract parameters from input
	if (input.inputs.size() < 2) {
		throw Exception(ExceptionType::INVALID,
		                "distributed_table requires at least 2 parameters: server_url and table_name");
	}

	string server_url = input.inputs[0].GetValue<string>();
	string remote_table_name = input.inputs[1].GetValue<string>();

	// For now, use hardcoded schema - in real implementation, this would query the server
	vector<LogicalType> column_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER};
	vector<string> column_names = {"id", "name", "value"};

	return_types = column_types;
	names = column_names;

	return make_uniq<DistributedTableBindData>(server_url, remote_table_name, column_types, column_names);
}

unique_ptr<GlobalTableFunctionState> DistributedTableFunction::InitGlobal(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	return make_uniq<DistributedTableGlobalState>();
}

unique_ptr<LocalTableFunctionState> DistributedTableFunction::InitLocal(ExecutionContext &context,
                                                                        TableFunctionInitInput &input,
                                                                        GlobalTableFunctionState *global_state) {
	return make_uniq<DistributedTableLocalState>();
}

void DistributedTableFunction::Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<DistributedTableBindData>();
	auto &local_state = data.local_state->Cast<DistributedTableLocalState>();

	if (local_state.finished) {
		output.SetCardinality(0);
		return;
	}

	// Create server instance (in real implementation, this would connect to remote server)
	static DistributedServer server;

	// Check if table exists on server
	if (!server.TableExists(bind_data.remote_table_name)) {
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	// Get data from server
	auto result = server.ScanTable(bind_data.remote_table_name, output.GetCapacity(), 0);

	if (result->HasError()) {
		throw Exception(ExceptionType::INTERNAL, "Distributed table scan error: " + result->GetError());
	}

	// Convert result to output chunk
	auto data_chunk = result->Fetch();
	if (data_chunk && data_chunk->size() > 0) {
		// Copy the data from the fetched chunk to our output chunk
		output.Reference(*data_chunk);
		local_state.finished = true; // For simplicity, we only return one chunk
	} else {
		output.SetCardinality(0);
		local_state.finished = true;
	}
}

double DistributedTableFunction::Progress(ClientContext &context, const FunctionData *bind_data,
                                          const GlobalTableFunctionState *global_state) {
	auto &state = global_state->Cast<DistributedTableGlobalState>();
	return state.finished ? 1.0 : 0.0;
}

} // namespace duckdb
