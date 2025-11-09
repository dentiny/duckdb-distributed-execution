#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Forward declaration.
struct PlanPartitionInfo;

// Generates SQL queries with partition predicates for distributed execution.
class PartitionSQLGenerator {
public:
	// Create partition SQL statement with WHERE clause for a specific partition.
	// Uses intelligent partitioning (range-based or modulo) based on plan analysis.
	static string CreatePartitionSQL(const string &sql, idx_t partition_id, idx_t total_partitions,
	                                 const PlanPartitionInfo &partition_info);

	// Inject WHERE clause into SQL at the correct position.
	// Handles queries with GROUP BY, HAVING, ORDER BY, LIMIT, etc.
	// If WHERE already exists, appends with AND.
	static string InjectWhereClause(const string &sql, const string &where_condition);
};

} // namespace duckdb
