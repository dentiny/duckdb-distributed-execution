#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterDuckherderFunctions(ExtensionLoader &loader);
void RegisterWorkerFragmentTestFunctions(ExtensionLoader &loader);

} // namespace duckdb
