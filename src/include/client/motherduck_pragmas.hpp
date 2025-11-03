#pragma once

#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class ClientContext;
class FunctionParameters;

// TODO(hjiang): Current implementation assumes hard-coded database and catalog type, remove.
class MotherduckPragmas {
public:
	static void RegisterPragmas(ExtensionLoader &loader);

private:
	static void RegisterRemoteTable(ClientContext &context, const FunctionParameters &parameters);
	static void UnregisterRemoteTable(ClientContext &context, const FunctionParameters &parameters);
};

} // namespace duckdb
