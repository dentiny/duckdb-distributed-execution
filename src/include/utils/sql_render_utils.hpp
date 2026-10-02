#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Forward declaration.
class LogicalAggregate;
class LogicalGet;
class TableCatalogEntry;
class TableFilter;
struct ReplacementBinding;

// Results on TIMESTAMPTZ and TIMETZ may depend on client settings such as TimeZone, which the server lacks.
bool DependsOnTimeZone(const LogicalType &type);

// ENUMs order by declaration but would compare as strings against a remote literal, and aliased or nested types may
// not render as valid remote SQL. Filters on these columns are evaluated locally instead.
bool SupportsRemoteFilterPushdown(const LogicalType &type);

// Translates a table filter into a SQL predicate on `column`.
// Returns an empty string for optional filters, which only prune data and are not needed for correctness.
string RemoteFilterToSQL(const TableFilter &filter, const string &column);

// Returns the quoted name and type of a physical or virtual column of `table`.
string GetColumnSQL(const TableCatalogEntry &table, column_t column_id, LogicalType &type);

// Renders the filters of `get` on `table` as SQL predicates, or returns false if one cannot be translated.
bool RenderScanFilters(const LogicalGet &get, const TableCatalogEntry &table, vector<string> &predicates);

// Renders `SELECT <select_list> FROM <table_name> [WHERE <predicates>]`.
string RenderSelectQuery(const vector<string> &select_list, const string &table_name, const vector<string> &predicates);

// Returns the scan `aggregate` reads, possibly through a projection, or nullptr.
optional_ptr<LogicalGet> GetAggregateScan(LogicalAggregate &aggregate);

struct RemoteAggregateQuery {
	string sql;
	// Groups followed by aggregates.
	vector<LogicalType> types;
	vector<string> names;
};

// Renders `aggregate` over `get`, its scan of `table`, as a query on `table_name`, or returns nullptr if it cannot be
// translated with identical semantics.
unique_ptr<RemoteAggregateQuery> RenderAggregateQuery(LogicalAggregate &aggregate, const LogicalGet &get,
                                                      const TableCatalogEntry &table, const string &table_name);

// Redirects the group and aggregate bindings of `aggregate` to the columns of the scan `table_index` returning
// the result of `RenderAggregateQuery`.
void ReplaceAggregateBindings(const LogicalAggregate &aggregate, idx_t table_index,
                              vector<ReplacementBinding> &replacements);

} // namespace duckdb
