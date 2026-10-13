#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include <optional>

namespace duckdb {

// Forward declarations.
class LogicalGet;
class LogicalOperator;

// Estimated bytes a Join input reads and, as the build side, holds in a hash table.
struct JoinInputCost {
	double scan_bytes;
	double build_bytes;
};

// Estimates the cost of `input`, a Join input reading `scan`. The scan reads every table row, since filters only drop
// rows after reading them. The build holds the filtered tuples, sized like DuckDB's build-side optimizer.
// Returns nothing without a cardinality estimate.
std::optional<JoinInputCost> EstimateJoinInputCost(LogicalOperator &input, LogicalGet &scan);

// Whether one of `num_tasks` tasks, reading its share of `partitioned` and all of each `replicated` input, finishes
// before a single node would. Each task's hash tables must fit in `max_build_bytes`.
bool ShouldPartitionJoin(const JoinInputCost &partitioned, const vector<JoinInputCost> &replicated, idx_t num_tasks,
                         double max_build_bytes);

// Remove `client_catalog.` qualifiers from `sql`, so names the client resolved against its attached catalog resolve
// against the server's default catalog.
string StripClientCatalog(const string &sql, const string &client_catalog);

} // namespace duckdb
