#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class QueryResult;

unique_ptr<QueryResult> ApplyCoordinatorResultMetadata(unique_ptr<QueryResult> result, vector<LogicalType> types,
                                                       const vector<string> &names);

} // namespace duckdb
