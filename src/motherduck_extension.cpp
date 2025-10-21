#define DUCKDB_EXTENSION_MAIN

#include "motherduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

namespace {

void MotherduckScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Motherduck " + name.GetString() + " 🐥");
	});
}

void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto motherduck_scalar_function =
	    ScalarFunction("motherduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, MotherduckScalarFun);
	loader.RegisterFunction(motherduck_scalar_function);
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
