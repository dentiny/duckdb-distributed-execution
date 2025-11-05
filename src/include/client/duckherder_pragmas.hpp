#pragma once

#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class ClientContext;
class FunctionParameters;
class DataChunk;
class ExpressionState;
class Vector;

// TODO(hjiang): Current implementation assumes hard-coded database and catalog type, remove.
class DuckherderPragmas {
public:
	static void RegisterPragmas(ExtensionLoader &loader);

private:
	static void RegisterRemoteTable(ClientContext &context, const FunctionParameters &parameters);
	static void UnregisterRemoteTable(ClientContext &context, const FunctionParameters &parameters);
	static void LoadExtension(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
