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

	// Deserialize a plan written by `SerializeLogicalPlan`, resolving its tables and functions in `context`.
	static unique_ptr<LogicalOperator> DeserializeLogicalPlan(ClientContext &context, const string &data);

	// Only the same DuckDB and Duckherder build can read a serialized plan.
	static string BuildId();
};

} // namespace duckdb
