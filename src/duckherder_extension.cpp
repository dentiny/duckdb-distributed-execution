#define DUCKDB_EXTENSION_MAIN

#include "distributed_server_function.hpp"
#include "duckdb.hpp"
#include "duckherder_extension.hpp"
#include "duckherder_pragmas.hpp"
#include "duckherder_storage.hpp"
#include "query_history_query_function.hpp"
#include "query_recorder_factory.hpp"

namespace duckdb {

namespace {

// Successful execution result.
constexpr bool SUCCESS = true;

// Clear stats for query recorder.
void ClearQueryRecorderStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	GetQueryRecorder().ClearQueryRecords();
	result.Reference(Value(SUCCESS));
}

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	// Register 'dh' as the storage extension prefix (for ATTACH DATABASE 'dh:' syntax)
	config.storage_extensions["dh"] = make_uniq<DuckherderStorageExtension>();
	// Also register as 'duckherder' for TYPE syntax (ATTACH DATABASE 'dh' (TYPE duckherder))
	config.storage_extensions["duckherder"] = make_uniq<DuckherderStorageExtension>();

	// Register pragma functions to register and unregister remote table.
	DuckherderPragmas::RegisterPragmas(loader);

	// Register function to get query stats.
	loader.RegisterFunction(GetQueryHistory());

	// Register function to clear query recorder stats.
	ScalarFunction clear_recorder_stats_function("dh_clear_query_recorder_stats",
	                                             /*arguments=*/ {},
	                                             /*return_type=*/LogicalType::BOOLEAN, ClearQueryRecorderStats);
	loader.RegisterFunction(clear_recorder_stats_function);

	// Register distributed server control functions, which could be local usage.
	loader.RegisterFunction(GetStartDistributedServerFunction());
	loader.RegisterFunction(GetStopDistributedServerFunction());
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
