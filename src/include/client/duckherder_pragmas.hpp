#pragma once

#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class ClientContext;
class DataChunk;
class Vector;
struct ExpressionState;
struct FunctionParameters;

// TODO(hjiang): Current implementation assumes hard-coded database and catalog type, remove.
class DuckherderPragmas {
public:
	static PragmaFunction GetUnregisterRemoteTableFunction();
	static ScalarFunction GetLoadExtensionFunction();

private:
	static void UnregisterRemoteTable(ClientContext &context, const FunctionParameters &parameters);
	static void LoadExtension(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
