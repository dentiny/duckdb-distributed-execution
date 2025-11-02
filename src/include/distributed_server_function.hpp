#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

// Register scalar functions for starting/stopping distributed server
// This allows SQL tests to start the server via: SELECT start_distributed_server();
void RegisterDistributedServerFunctions(ExtensionLoader &loader);

} // namespace duckdb
