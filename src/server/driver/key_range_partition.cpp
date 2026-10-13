#include "server/driver/key_range_partition.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "server/driver/plan_serializer.hpp"

#include <algorithm>

namespace duckdb {

namespace {

// Two scan columns that an inner Join compares for equality.
struct KeyPair {
	KeyRangeScan left;
	KeyRangeScan right;
};

bool SameColumn(const KeyRangeScan &a, const KeyRangeScan &b) {
	return a.table_index == b.table_index && a.column == b.column;
}

// Returns the scan in op whose table column binding reads, following plain column references through projections, and
// sets column to it. Returns nullptr for computed and virtual columns.
optional_ptr<LogicalGet> ResolveColumn(LogicalOperator &op, const ColumnBinding &binding, column_t &column) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		const auto &column_ids = get.GetColumnIds();
		if (get.table_index != binding.table_index || binding.column_index >= column_ids.size()) {
			return nullptr;
		}
		const auto &column_id = column_ids[binding.column_index];
		if (column_id.IsRowIdColumn() || column_id.IsVirtualColumn() || column_id.IsPushdownExtract()) {
			return nullptr;
		}
		column = column_id.GetPrimaryIndex();
		return get;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION &&
	    op.Cast<LogicalProjection>().table_index == binding.table_index) {
		auto &expr = *op.expressions[binding.column_index];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return nullptr;
		}
		return ResolveColumn(*op.children[0], expr.Cast<BoundColumnRefExpression>().binding, column);
	}
	for (auto &child : op.children) {
		auto result = ResolveColumn(*child, binding, column);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

// Collects the column pairs that inner Joins in op compare for equality, through filters, projections and inner Joins.
// A NULL key never matches, so key range filters may drop it.
void CollectKeyPairs(LogicalOperator &op, vector<KeyPair> &pairs) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER) {
			return;
		}
		for (const auto &condition : join.conditions) {
			if (condition.comparison != ExpressionType::COMPARE_EQUAL ||
			    condition.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    condition.right->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			KeyPair pair;
			auto left = ResolveColumn(*op.children[0], condition.left->Cast<BoundColumnRefExpression>().binding,
			                          pair.left.column);
			auto right = ResolveColumn(*op.children[1], condition.right->Cast<BoundColumnRefExpression>().binding,
			                           pair.right.column);
			if (left && right) {
				pair.left.table_index = left->table_index;
				pair.right.table_index = right->table_index;
				pairs.emplace_back(pair);
			}
		}
		break;
	}
	default:
		return;
	}
	for (auto &child : op.children) {
		CollectKeyPairs(*child, pairs);
	}
}

// Returns the columns that equal key through the equalities in pairs, including key itself.
vector<KeyRangeScan> EqualColumns(const KeyRangeScan &key, const vector<KeyPair> &pairs) {
	vector<KeyRangeScan> result {key};
	auto contains = [&](const KeyRangeScan &column) {
		for (const auto &member : result) {
			if (SameColumn(member, column)) {
				return true;
			}
		}
		return false;
	};
	bool changed = true;
	while (changed) {
		changed = false;
		for (const auto &pair : pairs) {
			if (contains(pair.left) != contains(pair.right)) {
				result.emplace_back(contains(pair.left) ? pair.right : pair.left);
				changed = true;
			}
		}
	}
	return result;
}

// Reads the minimum key of each row group of get's table, skipping those without keys. Returns false if a minimum is
// unknown or the row groups are not ordered on the key, since key ranges would not prune them.
bool ReadRowGroupMinimums(LogicalGet &get, column_t column, vector<Value> &minimums) {
	auto &table = *get.GetTable();
	auto &definition = table.GetColumn(LogicalIndex(column));
	if (definition.Generated() || !definition.Type().IsIntegral()) {
		return false;
	}
	auto row_groups = table.GetStorage().GetRowGroupCollection()->GetRowGroups();
	Value previous_max;
	for (auto segment = row_groups->GetRootSegment(); segment; segment = row_groups->GetNextSegment(*segment)) {
		unique_ptr<BaseStatistics> stats;
		try {
			stats = segment->GetNode().GetStatistics(definition.StorageOid());
		} catch (const std::exception &) {
			// The scan can still be split by rowid.
			return false;
		}
		if (!stats) {
			return false;
		}
		// Rows without a key never join, and every key range skips them.
		if (!stats->CanHaveNoNull()) {
			continue;
		}
		if (!NumericStats::HasMinMax(*stats)) {
			return false;
		}
		auto min = NumericStats::Min(*stats);
		if (!previous_max.IsNull() && min < previous_max) {
			return false;
		}
		previous_max = NumericStats::Max(*stats);
		minimums.emplace_back(std::move(min));
	}
	return !minimums.empty();
}

// Cuts the keys into at most max_tasks ranges with about as many row groups each. Repeated cuts are skipped, so a key
// spanning many row groups gives fewer ranges. Returns no cuts if only one range is left.
vector<Value> CutKeyRanges(const vector<Value> &minimums, idx_t max_tasks) {
	const auto num_tasks = MinValue<idx_t>(max_tasks, minimums.size());
	vector<Value> cuts;
	for (idx_t task = 1; task < num_tasks; ++task) {
		const auto &cut = minimums[task * minimums.size() / num_tasks];
		if (cut > minimums[0] && (cuts.empty() || cut > cuts.back())) {
			cuts.emplace_back(cut);
		}
	}
	return cuts;
}

// Restricts the scans of partition in op to the task's key range. The first range is open below and the last above.
void AddKeyRanges(LogicalOperator &op, const KeyRangePartition &partition, idx_t task) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		for (const auto &scan : partition.scans) {
			if (scan.table_index != get.table_index) {
				continue;
			}
			if (task > 0) {
				get.table_filters.PushFilter(
				    ColumnIndex(scan.column),
				    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, partition.cuts[task - 1]));
			}
			if (task < partition.cuts.size()) {
				get.table_filters.PushFilter(
				    ColumnIndex(scan.column),
				    make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, partition.cuts[task]));
			}
		}
	}
	for (auto &child : op.children) {
		AddKeyRanges(*child, partition, task);
	}
}

} // namespace

bool FindKeyRangePartition(LogicalOperator &input, LogicalGet &partitioned, const vector<reference<LogicalGet>> &scans,
                           const vector<JoinInputCost> &costs, idx_t max_tasks, KeyRangePartition &result) {
	vector<KeyPair> pairs;
	CollectKeyPairs(input, pairs);
	// Whether each scan column checked so far has its row groups ordered on it.
	struct CheckedColumn {
		KeyRangeScan column;
		bool ordered;
	};
	vector<CheckedColumn> checked;
	auto is_ordered = [&](LogicalGet &scan, column_t column) {
		const KeyRangeScan key {scan.table_index, column};
		for (const auto &entry : checked) {
			if (SameColumn(entry.column, key)) {
				return entry.ordered;
			}
		}
		vector<Value> unused;
		checked.push_back({key, ReadRowGroupMinimums(scan, column, unused)});
		return checked.back().ordered;
	};
	vector<column_t> tried;
	double best_bytes = 0;
	for (const auto &pair : pairs) {
		for (const auto &key : {pair.left, pair.right}) {
			if (key.table_index != partitioned.table_index ||
			    std::find(tried.begin(), tried.end(), key.column) != tried.end()) {
				continue;
			}
			tried.emplace_back(key.column);
			vector<Value> minimums;
			if (!ReadRowGroupMinimums(partitioned, key.column, minimums)) {
				continue;
			}
			const auto &key_type = partitioned.GetTable()->GetColumn(LogicalIndex(key.column)).Type();
			const auto equal_columns = EqualColumns(key, pairs);
			KeyRangePartition candidate;
			candidate.scans.emplace_back(key);
			double bytes = 0;
			for (idx_t idx = 0; idx < scans.size(); ++idx) {
				auto &scan = scans[idx].get();
				if (&scan == &partitioned) {
					continue;
				}
				for (const auto &column : equal_columns) {
					if (column.table_index == scan.table_index &&
					    scan.GetTable()->GetColumn(LogicalIndex(column.column)).Type() == key_type &&
					    is_ordered(scan, column.column)) {
						candidate.scans.emplace_back(column);
						bytes += costs[idx].scan_bytes;
						break;
					}
				}
			}
			if (candidate.scans.size() < 2 || bytes <= best_bytes) {
				continue;
			}
			candidate.cuts = CutKeyRanges(minimums, max_tasks);
			if (!candidate.cuts.empty()) {
				result = std::move(candidate);
				best_bytes = bytes;
			}
		}
	}
	return best_bytes > 0;
}

vector<string> SerializeKeyRangeTaskPlans(ClientContext &context, LogicalOperator &plan,
                                          const KeyRangePartition &partition) {
	vector<string> plans;
	plans.reserve(partition.cuts.size() + 1);
	for (idx_t task = 0; task <= partition.cuts.size(); ++task) {
		auto copy = plan.Copy(context);
		AddKeyRanges(*copy, partition, task);
		plans.emplace_back(PlanSerializer::SerializeLogicalPlan(*copy));
	}
	return plans;
}

} // namespace duckdb
