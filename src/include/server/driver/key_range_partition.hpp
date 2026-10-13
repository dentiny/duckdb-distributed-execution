#pragma once

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

// Finds the scans that inner equi-Joins in input tie to a key of partitioned and whose row groups are ordered on it,
// so they can be split with partitioned by key ranges. Picks the key whose scans read the most. Returns false if no
// scan qualifies or the key gives fewer than two ranges.
bool FindKeyRangePartition(LogicalOperator &input, LogicalGet &partitioned, const vector<reference<LogicalGet>> &scans,
                           const vector<JoinInputCost> &costs, idx_t max_tasks, KeyRangePartition &result);

// Serializes plan once per task, restricting each scan of partition to the task's key range.
vector<string> SerializeKeyRangeTaskPlans(ClientContext &context, LogicalOperator &plan,
                                          const KeyRangePartition &partition);

} // namespace duckdb
