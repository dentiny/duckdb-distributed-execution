#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

// Provides utility functions for serializing DuckDB internal structures.
class PlanSerializer {
public:
	// Serialize a logical operator to binary format.
	static string SerializeLogicalPlan(LogicalOperator &op);

	// Serialize a logical type to binary format.
	static string SerializeLogicalType(const LogicalType &type);
};

} // namespace duckdb
