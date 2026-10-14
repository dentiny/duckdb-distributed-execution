#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"
#include "server/driver/query_utils.hpp"

namespace duckdb {

class ClientContext;
class LogicalGet;
class LogicalOperator;

// A scan split by ranges of one of its columns.
struct KeyRangeScan {
	idx_t table_index;
	column_t column;
};

// Scans split by the same ranges of a Join key, so rows with equal keys go to the same task.
struct KeyRangePartition {
	vector<KeyRangeScan> scans;
	// Ascending; task i reads keys in [cuts[i - 1], cuts[i]).
	vector<Value> cuts;
};

// Finds the scans that equi-Joins in input tie to a key of partitioned and whose row groups are ordered on it,
// so they can be split with partitioned by key ranges. Picks the key whose scans read the most. Returns false if no
// scan qualifies or the key gives fewer than two ranges.
bool FindKeyRangePartition(LogicalOperator &input, LogicalGet &partitioned, const vector<reference<LogicalGet>> &scans,
                           const vector<JoinInputCost> &costs, idx_t max_tasks, KeyRangePartition &result);

// Whether a Join comparing two key columns with comparison only matches equal keys, which key ranges put in one task.
// NULL keys all go to the first task, so IS NOT DISTINCT FROM qualifies too.
bool IsKeyComparison(ExpressionType comparison);

// How the output of a plan is spread over tasks when some of its scans are split.
enum class SplitKind : uint8_t {
	// Every task computes the whole output.
	REPLICATED,
	// Each output row comes from exactly one task.
	SPLIT,
	// SPLIT, and rows with equal key values come from the same task.
	SPLIT_BY_KEY,
	// Splitting would change the output.
	INVALID
};

bool IsSplit(SplitKind kind);

// Returns how the output of op is spread over tasks when the scans of partition are split by its key ranges and the
// scan with table index rowid_scan, if valid, by rowid ranges.
SplitKind AnalyzeSplit(LogicalOperator &op, const KeyRangePartition &partition, optional_idx rowid_scan);

// Serializes plan once per task, restricting each scan of partition to the task's key range.
vector<string> SerializeKeyRangeTaskPlans(ClientContext &context, LogicalOperator &plan,
                                          const KeyRangePartition &partition);

} // namespace duckdb
