#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

#include <functional>

namespace duckdb {

// Forward declaration.
class LogicalAggregate;
class LogicalGet;
class TableCatalogEntry;
class TableFilter;

// ENUMs order by declaration but would compare as strings against a remote literal, and aliased or nested types may
// not render as valid remote SQL. Filters on these columns are evaluated locally instead.
bool SupportsRemoteFilterPushdown(const LogicalType &type);

// Translates a table filter into a SQL predicate on `column`.
// Returns an empty string for optional filters, which only prune data and are not needed for correctness.
string RemoteFilterToSQL(const TableFilter &filter, const string &column);

// Returns the quoted name and type of a physical or virtual column of `table`.
string GetColumnSQL(const TableCatalogEntry &table, const virtual_column_map_t &virtual_columns, column_t column_id,
                    LogicalType &type);

// Returns the quoted name and type of a scanned table column.
using RemoteColumnFunction = std::function<string(column_t column_id, LogicalType &type)>;

// Renders the filters of `get` as SQL predicates, or returns false if one cannot be translated.
bool RenderScanFilters(const LogicalGet &get, const RemoteColumnFunction &get_column, vector<string> &predicates);

// Returns the scan `aggregate` reads, possibly through a projection, or nullptr.
optional_ptr<LogicalGet> GetAggregateScan(LogicalAggregate &aggregate);

// Renders `aggregate` over `get`, its scan, as a query on `table_name` returning groups followed by aggregates, or
// returns an empty string if it cannot be translated with identical semantics.
string RenderAggregateQuery(LogicalAggregate &aggregate, const LogicalGet &get, const RemoteColumnFunction &get_column,
                            const string &table_name, vector<LogicalType> &types, vector<string> &names);

} // namespace duckdb
