#pragma once

namespace duckdb {

// Forward declarations.
class PhysicalOperator;
class LogicalOperator;

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

} // namespace duckdb
