#include "distributed_table_scan_function.hpp"

#include "distributed_server.hpp"
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
	return make_uniq<DistributedTableScanLocalState>();
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
	auto &server = DistributedServer::GetInstance();
	if (!server.TableExists(bind_data.remote_table_name)) {
		DUCKDB_LOG_DEBUG(db, StringUtil::Format("Table %s does not exist on server", bind_data.remote_table_name));
		output.SetCardinality(0);
		local_state.finished = true;
		return;
	}

	DUCKDB_LOG_DEBUG(db, StringUtil::Format("Fetching data from server for table: %s", bind_data.remote_table_name));
	auto result = server.ScanTable(bind_data.remote_table_name, output.GetCapacity(), 0);

	if (result->HasError()) {
		throw Exception(ExceptionType::INTERNAL, "Distributed table scan error: " + result->GetError());
	}

	auto data_chunk = result->Fetch();
	if (data_chunk && data_chunk->size() > 0) {
		output.Reference(*data_chunk);
		// TODO(hjiang): For simplicity, we only return one chunk.
		local_state.finished = true;
	} else {
		output.SetCardinality(0);
		local_state.finished = true;
	}
}

} // namespace duckdb
