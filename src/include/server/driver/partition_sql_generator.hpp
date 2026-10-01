#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Generates SQL queries with partition predicates for distributed execution.
class PartitionSQLGenerator {
public:
	// Inject WHERE clause into SQL at the correct position.
	// Handles queries with GROUP BY, HAVING, ORDER BY, LIMIT, etc.
	// If WHERE already exists, appends with AND.
	static string InjectWhereClause(const string &sql, const string &where_condition);
};

} // namespace duckdb
