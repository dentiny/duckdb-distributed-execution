#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Forward declarations.
class PhysicalOperator;
class LogicalOperator;

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

// Remove `client_catalog.` qualifiers from `sql`, so names the client resolved against its attached catalog resolve
// against the server's default catalog.
string StripClientCatalog(const string &sql, const string &client_catalog);

} // namespace duckdb
