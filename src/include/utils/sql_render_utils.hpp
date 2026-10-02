#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

#include <functional>

namespace duckdb {

// Forward declaration.
class LogicalAggregate;
class LogicalGet;
class TableFilter;

// ENUMs order by declaration but would compare as strings against a remote literal, and aliased or nested types may
// not render as valid remote SQL. Filters on these columns are evaluated locally instead.
bool SupportsRemoteFilterPushdown(const LogicalType &type);

// Whether `RemoteFilterToSQL` can translate `filter`.
bool IsRemoteFilter(const TableFilter &filter);

// Translates a table filter into a SQL predicate on `column`.
// Returns an empty string for optional filters, which only prune data and are not needed for correctness.
string RemoteFilterToSQL(const TableFilter &filter, const string &column);

// Returns the quoted name and type of a scanned table column.
using RemoteColumnFunction = std::function<string(column_t column_id, LogicalType &type)>;

// Renders the filters of `get` as SQL predicates, or returns false if one cannot be translated.
bool RenderScanFilters(const LogicalGet &get, const RemoteColumnFunction &get_column, vector<string> &predicates);

// Returns the scan `aggregate` reads, possibly through a projection, or nullptr.
optional_ptr<LogicalGet> GetAggregateScan(LogicalAggregate &aggregate);

// Renders `aggregate` over its scan as a query on `table_name` returning groups followed by aggregates, or returns an
// empty string if it cannot be translated with identical semantics.
string RenderAggregateQuery(LogicalAggregate &aggregate, const RemoteColumnFunction &get_column,
                            const string &table_name, vector<LogicalType> &types, vector<string> &names);

} // namespace duckdb
