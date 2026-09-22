#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterDuckherderFunctions(ExtensionLoader &loader);

} // namespace duckdb
