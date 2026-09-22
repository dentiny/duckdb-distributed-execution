#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckherder_extension.hpp"
#include "duckherder_functions.hpp"
#include "duckherder_extension_instance_state.hpp"
#include "duckherder_storage.hpp"

namespace duckdb {

namespace {

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "duckherder", make_shared_ptr<DuckherderStorageExtension>());

	// Set extension state.
	SetInstanceState(db, make_shared_ptr<DuckherderInstanceState>());

	RegisterDuckherderFunctions(loader);
}

} // namespace

void DuckherderExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DuckherderExtension::Name() {
	return "duckherder";
}

std::string DuckherderExtension::Version() const {
#ifdef EXT_VERSION_DUCKHERDER
	return EXT_VERSION_DUCKHERDER;
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
