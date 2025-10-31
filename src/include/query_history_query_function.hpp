#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Table function to get query history.
TableFunction GetQueryHistory();

} // namespace duckdb
