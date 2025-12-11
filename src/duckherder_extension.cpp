#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckherder_extension.hpp"
#include "duckherder_extension_instance_state.hpp"
#include "duckherder_pragmas.hpp"
#include "duckherder_storage.hpp"
#include "query_history_query_function.hpp"
#include "query_execution_stats_query_function.hpp"
#include "server/driver/distributed_server_function.hpp"

namespace duckdb {

namespace {

// Successful execution result.
constexpr bool SUCCESS = true;

// Get database instance from expression state.
// Returned instance ownership lies in the given [`state`].
DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
}

// Clear stats for query recorder.
void ClearQueryRecorderStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &duckdb_instance = GetDatabaseInstance(state);
	auto &instance_state = GetInstanceStateOrThrow(duckdb_instance);
	instance_state.GetQueryRecorder()->ClearQueryRecords();
	result.Reference(Value(SUCCESS));
}

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["duckherder"] = make_uniq<DuckherderStorageExtension>();

	// Set extension state.
	SetInstanceState(db, make_shared_ptr<DuckherderInstanceState>());

	// Register pragma functions to register and unregister remote table.
	DuckherderPragmas::RegisterPragmas(loader);

	// Register function to get query stats.
	loader.RegisterFunction(GetQueryHistory());

	// Register function to get query execution stats from driver.
	loader.RegisterFunction(GetQueryExecutionStats());

	// Register function to clear query recorder stats.
	ScalarFunction clear_recorder_stats_function("duckherder_clear_query_recorder_stats",
	                                             /*arguments=*/ {},
	                                             /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN},
	                                             ClearQueryRecorderStats);
	loader.RegisterFunction(clear_recorder_stats_function);

	// Register distributed server control functions, which could be local usage.
	loader.RegisterFunction(GetStartLocalServerFunction());
	loader.RegisterFunction(GetStopLocalServerFunction());
	loader.RegisterFunction(GetRegisterOrReplaceDriverFunction());

	// Register worker management functions.
	loader.RegisterFunction(GetWorkerCountFunction());
	loader.RegisterFunction(GetRegisterWorkerFunction());
	loader.RegisterFunction(GetStartStandaloneWorkerFunction());
}

} // namespace

void DuckherderExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DuckherderExtension::Name() {
	return "duckherder";
}

std::string DuckherderExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckherder, loader) {
	duckdb::LoadInternal(loader);
}
}
