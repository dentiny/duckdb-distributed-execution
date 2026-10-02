#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Whether whole queries are pushed down; otherwise only scans and aggregates are.
inline constexpr const char *QUERY_PUSHDOWN_SETTING = "duckherder_query_pushdown";

// Replaces a SELECT that only reads tables of one remote database with a single remote scan running the whole query
// on the server, which decides how to execute and distribute it. Only the final result is transferred to the client.
// Plans depending on client state, such as local tables, views, macros or variables, are kept.
OptimizerExtension GetDistributedQueryPushdownExtension();

} // namespace duckdb
