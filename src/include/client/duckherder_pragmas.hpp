#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class DataChunk;
class Vector;
struct ExpressionState;

// TODO(hjiang): Current implementation assumes hard-coded database and catalog type, remove.
class DuckherderPragmas {
public:
	static ScalarFunction GetLoadExtensionFunction();

private:
	static void LoadExtension(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
