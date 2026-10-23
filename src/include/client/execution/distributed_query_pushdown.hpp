#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/table_function.hpp"
#include <string_view>

namespace duckdb {

class Catalog;
class ClientContext;
class ParsedExpression;
class QueryNode;
class TableRef;

// Whether whole queries are pushed down; otherwise only scans and aggregates are.
inline constexpr std::string_view QUERY_PUSHDOWN_SETTING = "duckherder_query_pushdown";

// DuckDB's RemotePushdownOptimizer asks these whether the server resolves and evaluates a parsed construct like the
// client does. Constructs depending on client state, such as parameters, sequences or local types, stay on the client.
bool SupportsQueryPushdown(const ParsedExpression &expression);
bool SupportsQueryPushdown(const TableRef &ref);
bool SupportsQueryPushdown(const QueryNode &node);

// Returns a `duckherder_query` call running `node`, which only reads tables of `catalog`, on the server.
unique_ptr<TableRef> CreateRemoteQueryRef(ClientContext &context, Catalog &catalog, unique_ptr<QueryNode> node);

// `duckherder_query(catalog, sql)` runs a SELECT on the server of a Duckherder database, which decides how to execute
// and distribute it. Queries whose result depends on client settings or the client's time zone run on the client.
TableFunction GetRemoteQueryFunction();

} // namespace duckdb
