#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "motherduck_extension.hpp"
#include "motherduck_pragmas.hpp"
#include "motherduck_storage.hpp"
#include "query_history_query_function.hpp"
#include "query_recorder_factory.hpp"
#include "distributed_server_function.hpp"

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
	config.storage_extensions["motherduck"] = make_uniq<MotherduckStorageExtension>();

	// Register pragma functions to register and unregister remote table.
	MotherduckPragmas::RegisterPragmas(loader);

	// Register function to get query stats.
	loader.RegisterFunction(GetQueryHistory());

	// Register function to clear query recorder stats.
	ScalarFunction clear_recorder_stats_function("md_clear_query_recorder_stats",
	                                             /*arguments=*/ {},
	                                             /*return_type=*/LogicalType::BOOLEAN, ClearQueryRecorderStats);
	loader.RegisterFunction(clear_recorder_stats_function);

	// Register distributed server control functions for SQL tests
	RegisterDistributedServerFunctions(loader);
}

} // namespace

void MotherduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string MotherduckExtension::Name() {
	return "motherduck";
}

std::string MotherduckExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(motherduck, loader) {
	duckdb::LoadInternal(loader);
}
}
