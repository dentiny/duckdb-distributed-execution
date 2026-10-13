#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include <optional>

namespace duckdb {

// Forward declarations.
class LogicalGet;
class LogicalOperator;

// Estimated bytes of a Join input: read by its scan, and held as a hash table build side.
struct JoinInputCost {
	double scan_bytes;
	double build_bytes;
};

// Estimates the cost of input, a Join input reading scan. The scan reads every row of the table, as filters only drop
// rows it has read, at the width of the columns it reads. The build holds the projected tuples input passes to the
// Join after its filters, with the same small penalties for variable-size payloads as DuckDB's build-side optimizer.
// Returns nothing if neither operator has an estimated cardinality.
std::optional<JoinInputCost> EstimateJoinInputCost(LogicalOperator &input, LogicalGet &scan);

// Whether a Join finishes sooner as num_tasks tasks, each reading its share of partitioned and all of every replicated
// input, than on one node of the same size. A hash Join builds on its smaller input. Every task reads the replicated
// inputs from the same storage, so each is charged for half of the other tasks' copies. A task's replicated hash tables
// must fit in max_build_bytes.
bool ShouldPartitionJoin(const JoinInputCost &partitioned, const vector<JoinInputCost> &replicated, idx_t num_tasks,
                         double max_build_bytes);

// Remove `client_catalog.` qualifiers from `sql`, so names the client resolved against its attached catalog resolve
// against the server's default catalog.
string StripClientCatalog(const string &sql, const string &client_catalog);

} // namespace duckdb
