#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "motherduck_extension.hpp"
#include "motherduck_pragmas.hpp"
#include "motherduck_storage.hpp"
#include "query_history_query_function.hpp"

namespace duckdb {

namespace {

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["motherduck"] = make_uniq<MotherduckStorageExtension>();

	// Register pragma functions to register and unregister remote table.
	MotherduckPragmas::RegisterPragmas(loader);

	// Register function to get query stats.
	loader.RegisterFunction(GetQueryHistory());
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
