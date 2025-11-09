#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! PlanSerializer: Utility functions for serializing DuckDB internal structures
//! Used for potential future plan-based distribution (vs SQL-based)
class PlanSerializer {
public:
	//! Serialize a logical operator to binary format
	static string SerializeLogicalPlan(LogicalOperator &op);

	//! Serialize a logical type to binary format
	static string SerializeLogicalType(const LogicalType &type);
};

} // namespace duckdb
