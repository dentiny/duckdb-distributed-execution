#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Table function to get query execution statistics from the distributed server.
TableFunction GetQueryExecutionStats();

} // namespace duckdb
