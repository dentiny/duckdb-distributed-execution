#pragma once

#include "duckdb/common/string.hpp"

#include <optional>

namespace duckdb {

// Forward declarations.
class LogicalGet;
class PhysicalOperator;
class LogicalOperator;
class SelectStatement;

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

// Whether the client SQL has the simple two-table inner join shape supported by row-group partitioning.
bool IsSimplePartitionedJoin(const SelectStatement &statement);

// Estimated bytes of a Join input, as scanned and as a hash table build side.
struct JoinInputCost {
	double scan_bytes;
	double build_bytes;
};

// Estimates the projected tuples input, a Join input reading scan, passes to the Join after its filters. Variable-size
// payloads use the same small penalties as DuckDB's build-side optimizer. Returns nothing if neither operator has an
// estimated cardinality.
std::optional<JoinInputCost> EstimateJoinInputCost(LogicalOperator &input, LogicalGet &scan);

// Remove `client_catalog.` qualifiers from `sql`, so names the client resolved against its attached catalog resolve
// against the server's default catalog.
string StripClientCatalog(const string &sql, const string &client_catalog);

} // namespace duckdb
