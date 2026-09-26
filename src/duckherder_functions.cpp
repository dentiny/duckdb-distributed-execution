#include "duckherder_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckherder_extension_instance_state.hpp"
#include "duckherder_pragmas.hpp"
#include "duckherder_remote_query.hpp"
#include "query_execution_stats_query_function.hpp"
#include "query_history_query_function.hpp"
#include "server/driver/distributed_server_function.hpp"

namespace duckdb {

namespace {

constexpr bool SUCCESS = true;

void AddDescription(CreateFunctionInfo &info, vector<string> parameter_names, string description,
                    vector<string> examples, vector<string> categories) {
	FunctionDescription function_description;
	function_description.parameter_names = std::move(parameter_names);
	function_description.description = std::move(description);
	function_description.examples = std::move(examples);
	function_description.categories = std::move(categories);
	info.descriptions.push_back(std::move(function_description));
}

void RegisterScalarFunction(ExtensionLoader &loader, ScalarFunction function, vector<string> parameter_names,
                            string description, vector<string> examples, vector<string> categories) {
	CreateScalarFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	AddDescription(info, std::move(parameter_names), std::move(description), std::move(examples),
	               std::move(categories));
	loader.RegisterFunction(std::move(info));
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, vector<string> parameter_names,
                           string description, vector<string> examples, vector<string> categories) {
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	AddDescription(info, std::move(parameter_names), std::move(description), std::move(examples),
	               std::move(categories));
	loader.RegisterFunction(std::move(info));
}

void RegisterPragmaFunction(ExtensionLoader &loader, PragmaFunction function, vector<string> parameter_names,
                            string description, vector<string> examples, vector<string> categories) {
	auto function_name = function.name;
	PragmaFunctionSet functions(function_name);
	functions.AddFunction(std::move(function));
	CreatePragmaFunctionInfo info(std::move(function_name), std::move(functions));
	AddDescription(info, std::move(parameter_names), std::move(description), std::move(examples),
	               std::move(categories));

	auto &db = loader.GetDatabaseInstance();
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreatePragmaFunction(transaction, info);
}

DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
}

void ClearQueryRecorderStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &duckdb_instance = GetDatabaseInstance(state);
	auto &instance_state = GetInstanceStateOrThrow(duckdb_instance);
	instance_state.GetQueryRecorder()->ClearQueryRecords();
	result.Reference(Value(SUCCESS));
}

ScalarFunction GetClearQueryRecorderStatsFunction() {
	return ScalarFunction("duckherder_clear_query_recorder_stats",
	                      /*arguments=*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, ClearQueryRecorderStats);
}

} // namespace

void RegisterDuckherderFunctions(ExtensionLoader &loader) {
	RegisterPragmaFunction(
	    loader, DuckherderPragmas::GetRegisterRemoteTableFunction(),
	    /*parameter_names=*/ {"local_table_name", "remote_table_name"},
	    /*description=*/"Registers a local table name as a mapping to a table on the attached Duckherder server.",
	    /*examples=*/ {"PRAGMA duckherder_register_remote_table('orders', 'remote_orders');"},
	    /*categories=*/ {"duckherder", "distributed_execution", "catalog"});
	RegisterPragmaFunction(loader, DuckherderPragmas::GetUnregisterRemoteTableFunction(),
	                       /*parameter_names=*/ {"local_table_name"},
	                       /*description=*/"Removes a remote table mapping from the attached Duckherder catalog.",
	                       /*examples=*/ {"PRAGMA duckherder_unregister_remote_table('orders');"},
	                       /*categories=*/ {"duckherder", "distributed_execution", "catalog"});
	RegisterScalarFunction(
	    loader, DuckherderPragmas::GetLoadExtensionFunction(),
	    /*parameter_names=*/ {"extension_name"},
	    /*description=*/"Loads an extension on the attached Duckherder server and attempts to load it on the client.",
	    /*examples=*/ {"SELECT duckherder_load_extension('parquet');"},
	    /*categories=*/ {"duckherder", "distributed_execution", "extension"});
	RegisterTableFunction(loader, GetQueryHistory(),
	                      /*parameter_names=*/ {},
	                      /*description=*/"Returns recorded SQL queries and their observed execution latencies.",
	                      /*examples=*/ {"SELECT * FROM duckherder_get_query_history();"},
	                      /*categories=*/ {"duckherder", "distributed_execution", "observability"});
	RegisterTableFunction(
	    loader, GetQueryExecutionStats(),
	    /*parameter_names=*/ {},
	    /*description=*/
	    "Returns execution mode, merge strategy, duration, worker, task, and start-time statistics for distributed "
	    "queries.",
	    /*examples=*/ {"SELECT * FROM duckherder_get_query_execution_stats();"},
	    /*categories=*/ {"duckherder", "distributed_execution", "observability"});
	RegisterTableFunction(loader, GetDuckherderRemoteQueryFunction(),
	                      /*parameter_names=*/ {"catalog_name", "sql", "modification"},
	                      /*description=*/"Internal execution function for pushed-down Duckherder DML statements.",
	                      /*examples=*/ {},
	                      /*categories=*/ {"duckherder", "distributed_execution", "internal"});
	RegisterScalarFunction(
	    loader, GetClearQueryRecorderStatsFunction(),
	    /*parameter_names=*/ {},
	    /*description=*/"Clears all query history records collected by the Duckherder query recorder.",
	    /*examples=*/ {"SELECT duckherder_clear_query_recorder_stats();"},
	    /*categories=*/ {"duckherder", "distributed_execution", "observability"});
	RegisterScalarFunction(
	    loader, GetStartLocalServerFunction(),
	    /*parameter_names=*/ {"port"},
	    /*description=*/"Starts a local Duckherder driver server on the given port, optionally with local workers.",
	    /*examples=*/ {"SELECT duckherder_start_local_server(8815, 4);"},
	    /*categories=*/ {"duckherder", "distributed_execution", "server"});
	RegisterScalarFunction(
	    loader, GetStopLocalServerFunction(),
	    /*parameter_names=*/ {},
	    /*description=*/"Stops the local Duckherder driver server and its managed standalone workers.",
	    /*examples=*/ {"SELECT duckherder_stop_local_server();"},
	    /*categories=*/ {"duckherder", "distributed_execution", "server"});
	RegisterScalarFunction(
	    loader, GetRegisterOrReplaceDriverFunction(),
	    /*parameter_names=*/ {"driver_id", "location"},
	    /*description=*/"Registers a driver node, replacing the currently registered driver if one exists.",
	    /*examples=*/
	    {"SELECT duckherder_register_or_replace_driver('driver-1', 'grpc://localhost:8815');"},
	    /*categories=*/ {"duckherder", "distributed_execution", "server"});
	RegisterScalarFunction(
	    loader, GetWorkerCountFunction(),
	    /*parameter_names=*/ {},
	    /*description=*/"Returns the number of workers registered with the local Duckherder driver server.",
	    /*examples=*/ {"SELECT duckherder_get_worker_count();"},
	    /*categories=*/ {"duckherder", "distributed_execution", "worker"});
	RegisterScalarFunction(loader, GetRegisterWorkerFunction(),
	                       /*parameter_names=*/ {"worker_id", "location"},
	                       /*description=*/"Registers a worker node with the local Duckherder driver server.",
	                       /*examples=*/ {"SELECT duckherder_register_worker('worker-1', 'grpc://localhost:8816');"},
	                       /*categories=*/ {"duckherder", "distributed_execution", "worker"});
	RegisterScalarFunction(loader, GetStartStandaloneWorkerFunction(),
	                       /*parameter_names=*/ {"port"},
	                       /*description=*/"Starts a standalone Duckherder worker on the given port.",
	                       /*examples=*/ {"SELECT duckherder_start_standalone_worker(8816);"},
	                       /*categories=*/ {"duckherder", "distributed_execution", "worker"});
}

} // namespace duckdb
