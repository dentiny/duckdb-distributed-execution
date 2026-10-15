#include "server/driver/key_range_partition.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "server/driver/plan_serializer.hpp"

namespace duckdb {

namespace {

// Two scan columns that a Join compares for equality.
struct KeyPair {
	KeyRangeScan left;
	KeyRangeScan right;
};

bool SameColumn(const KeyRangeScan &a, const KeyRangeScan &b) {
	return a.table_index == b.table_index && a.column == b.column;
}

bool ContainsColumn(const vector<KeyRangeScan> &columns, const KeyRangeScan &column) {
	for (const auto &member : columns) {
		if (SameColumn(member, column)) {
			return true;
		}
	}
	return false;
}

// A DELIM_GET and the DELIM_JOIN whose duplicate-eliminated columns it reads, with how its rows are spread over tasks.
// join is null if several DELIM_JOINs have the DELIM_GET below them, so its source is unclear.
struct DelimSource {
	idx_t table_index;
	optional_ptr<LogicalComparisonJoin> join;
	SplitKind kind;
};

// The index of the input of join whose duplicate-eliminated columns its DELIM_GETs, below the other input, read.
idx_t DelimInputIndex(const LogicalComparisonJoin &join) {
	return join.delim_flipped ? 1 : 0;
}

void CollectDelimGets(LogicalOperator &op, LogicalComparisonJoin &join, vector<DelimSource> &sources) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		const auto table_index = op.Cast<LogicalDelimGet>().table_index;
		bool found = false;
		for (auto &source : sources) {
			if (source.table_index == table_index) {
				source.join = nullptr;
				found = true;
			}
		}
		if (!found) {
			sources.push_back({table_index, join, SplitKind::REPLICATED});
		}
	}
	for (auto &child : op.children) {
		CollectDelimGets(*child, join, sources);
	}
}

// Collects the DELIM_GETs below each DELIM_JOIN in op.
void CollectDelimSources(LogicalOperator &op, vector<DelimSource> &sources) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		CollectDelimGets(*join.children[1 - DelimInputIndex(join)], join, sources);
	}
	for (auto &child : op.children) {
		CollectDelimSources(*child, sources);
	}
}

optional_ptr<DelimSource> FindDelimSource(vector<DelimSource> &sources, idx_t table_index) {
	for (auto &source : sources) {
		if (source.table_index == table_index) {
			return source;
		}
	}
	return nullptr;
}

// Returns the scan in op whose table column binding reads, following plain column references through projections,
// aggregate groups and DELIM_GETs, and sets column to it. Returns nullptr for computed and virtual columns.
optional_ptr<LogicalGet> ResolveColumn(LogicalOperator &op, const ColumnBinding &binding, column_t &column,
                                       vector<DelimSource> &delims) {
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
		return ResolveColumn(*op.children[0], expr.Cast<BoundColumnRefExpression>().binding, column, delims);
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
	    op.Cast<LogicalAggregate>().group_index == binding.table_index) {
		auto &aggregate = op.Cast<LogicalAggregate>();
		// With grouping sets, a group column is NULL in the rows of the sets without it.
		if (aggregate.grouping_sets.size() > 1 ||
		    aggregate.groups[binding.column_index]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return nullptr;
		}
		return ResolveColumn(*op.children[0],
		                     aggregate.groups[binding.column_index]->Cast<BoundColumnRefExpression>().binding, column,
		                     delims);
	}
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET &&
	    op.Cast<LogicalDelimGet>().table_index == binding.table_index) {
		auto source = FindDelimSource(delims, binding.table_index);
		if (!source || !source->join || binding.column_index >= source->join->duplicate_eliminated_columns.size()) {
			return nullptr;
		}
		auto &expr = *source->join->duplicate_eliminated_columns[binding.column_index];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return nullptr;
		}
		return ResolveColumn(*source->join->children[DelimInputIndex(*source->join)],
		                     expr.Cast<BoundColumnRefExpression>().binding, column, delims);
	}
	for (idx_t idx = 0; idx < op.children.size(); ++idx) {
		// A key an outer Join pads with NULL is NULL in every task, not only the first.
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			const auto join_type = op.Cast<LogicalJoin>().join_type;
			if (idx == 0 ? IsRightOuterJoin(join_type) : IsLeftOuterJoin(join_type) || join_type == JoinType::SINGLE) {
				continue;
			}
		}
		auto result = ResolveColumn(*op.children[idx], binding, column, delims);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

// Resolves the scan columns that condition of join compares as keys. Returns false if it compares anything else.
bool ResolveKeyPair(LogicalComparisonJoin &join, const JoinCondition &condition, KeyPair &pair,
                    vector<DelimSource> &delims) {
	if (!IsKeyComparison(condition.comparison) ||
	    condition.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
	    condition.right->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto left = ResolveColumn(*join.children[0], condition.left->Cast<BoundColumnRefExpression>().binding,
	                          pair.left.column, delims);
	auto right = ResolveColumn(*join.children[1], condition.right->Cast<BoundColumnRefExpression>().binding,
	                           pair.right.column, delims);
	if (!left || !right) {
		return false;
	}
	pair.left.table_index = left->table_index;
	pair.right.table_index = right->table_index;
	return true;
}

// Collects the column pairs that Joins in op compare for equality. AnalyzeSplit decides whether splitting by them is
// correct.
void CollectKeyPairs(LogicalOperator &op, vector<KeyPair> &pairs, vector<DelimSource> &delims) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (const auto &condition : join.conditions) {
			KeyPair pair;
			if (ResolveKeyPair(join, condition, pair, delims)) {
				pairs.emplace_back(pair);
			}
		}
	}
	for (auto &child : op.children) {
		CollectKeyPairs(*child, pairs, delims);
	}
}

// Returns the columns that equal key through the equalities in pairs, including key itself.
vector<KeyRangeScan> EqualColumns(const KeyRangeScan &key, const vector<KeyPair> &pairs) {
	vector<KeyRangeScan> result {key};
	bool changed = true;
	while (changed) {
		changed = false;
		for (const auto &pair : pairs) {
			if (ContainsColumn(result, pair.left) != ContainsColumn(result, pair.right)) {
				result.emplace_back(ContainsColumn(result, pair.left) ? pair.right : pair.left);
				changed = true;
			}
		}
	}
	return result;
}

// Whether type's row group minimums and maximums are exact and ordered like its comparisons, so key ranges prune row
// groups. TIME_TZ statistics are not ordered by instant, floating-point ones have NaN and -0.0, and string ones are
// truncated, so we exclude them.
bool IsRangeKeyType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
		return true;
	default:
		return type.IsIntegral() || LogicalType::TypeIsTimestamp(type);
	}
}

// Reads the minimum key of each row group of get's table, skipping those without keys. Returns false if a minimum is
// unknown or the row groups are not ordered on the key, since key ranges would not prune them.
bool ReadRowGroupMinimums(LogicalGet &get, column_t column, vector<Value> &minimums) {
	auto &table = *get.GetTable();
	auto &definition = table.GetColumn(LogicalIndex(column));
	if (definition.Generated() || !IsRangeKeyType(definition.Type())) {
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
		// Row groups of NULL keys give no minimum; the first range reads them.
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

bool HasNotNullConstraint(TableCatalogEntry &table, column_t column) {
	for (const auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL &&
		    constraint->Cast<NotNullConstraint>().index == LogicalIndex(column)) {
			return true;
		}
	}
	return false;
}

// Keeps the keys below cut and the NULL keys. A ConjunctionOrFilter selects rows in time quadratic in the rows passing,
// so it only prunes row groups and an expression filter selects the rows.
void PushBelowOrNull(LogicalGet &get, column_t column, const Value &cut) {
	auto or_null = make_uniq<ConjunctionOrFilter>();
	or_null->child_filters.emplace_back(make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, cut));
	or_null->child_filters.emplace_back(make_uniq<IsNullFilter>());
	get.table_filters.PushFilter(ColumnIndex(column), make_uniq<OptionalFilter>(std::move(or_null)));

	const auto &type = get.GetTable()->GetColumn(LogicalIndex(column)).Type();
	auto key = make_uniq<BoundReferenceExpression>(type, 0ULL);
	auto below = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, key->Copy(),
	                                                  make_uniq<BoundConstantExpression>(cut));
	auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
	is_null->children.emplace_back(std::move(key));
	get.table_filters.PushFilter(ColumnIndex(column),
	                             make_uniq<ExpressionFilter>(make_uniq<BoundConjunctionExpression>(
	                                 ExpressionType::CONJUNCTION_OR, std::move(below), std::move(is_null))));
}

// Restricts the scans of partition in op to the task's key range. The first range is open below and holds NULL keys,
// so rows without a key are read once; the last range is open above.
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
			if (task == partition.cuts.size()) {
				continue;
			}
			const auto &cut = partition.cuts[task];
			if (task == 0 && !HasNotNullConstraint(*get.GetTable(), scan.column)) {
				PushBelowOrNull(get, scan.column, cut);
			} else {
				get.table_filters.PushFilter(ColumnIndex(scan.column),
				                             make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, cut));
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
	vector<DelimSource> delims;
	CollectDelimSources(input, delims);
	vector<KeyPair> pairs;
	CollectKeyPairs(input, pairs, delims);
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

bool IsKeyComparison(ExpressionType comparison) {
	return comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

bool IsSplit(SplitKind kind) {
	return kind == SplitKind::SPLIT || kind == SplitKind::SPLIT_BY_KEY;
}

namespace {

// Whether a condition of join equates a key column of its left input with one of its right input.
bool JoinsOnKey(LogicalComparisonJoin &join, const KeyRangePartition &partition, vector<DelimSource> &delims) {
	for (const auto &condition : join.conditions) {
		KeyPair pair;
		if (ResolveKeyPair(join, condition, pair, delims) && ContainsColumn(partition.scans, pair.left) &&
		    ContainsColumn(partition.scans, pair.right)) {
			return true;
		}
	}
	return false;
}

// Whether one of groups, evaluated over input, is a key column, so each task holds whole groups.
bool GroupsByKey(LogicalOperator &input, const vector<unique_ptr<Expression>> &groups,
                 const KeyRangePartition &partition, vector<DelimSource> &delims) {
	for (const auto &group : groups) {
		if (group->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		KeyRangeScan column;
		auto scan = ResolveColumn(input, group->Cast<BoundColumnRefExpression>().binding, column.column, delims);
		if (scan) {
			column.table_index = scan->table_index;
			if (ContainsColumn(partition.scans, column)) {
				return true;
			}
		}
	}
	return false;
}

SplitKind Analyze(LogicalOperator &op, const KeyRangePartition &partition, optional_idx rowid_scan,
                  vector<DelimSource> &delims) {
	vector<SplitKind> children(op.children.size(), SplitKind::REPLICATED);
	// A DELIM_JOIN's DELIM_GETs hold the distinct keys of its other input in each task, so that input comes first.
	idx_t first = 0;
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		first = DelimInputIndex(op.Cast<LogicalComparisonJoin>());
	}
	for (idx_t step = 0; step < op.children.size(); ++step) {
		const auto idx = (first + step) % op.children.size();
		children[idx] = Analyze(*op.children[idx], partition, rowid_scan, delims);
		if (children[idx] == SplitKind::INVALID) {
			return SplitKind::INVALID;
		}
		if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN && step == 0) {
			// When that input is split by one of the duplicate-eliminated columns, each task's DELIM_GETs hold only its
			// own keys. Otherwise tasks may share keys, which is only correct if the rest of the subquery is
			// replicated: the final Join then finds every match of a row in its own task.
			auto &join = op.Cast<LogicalComparisonJoin>();
			const auto kind =
			    children[idx] == SplitKind::SPLIT_BY_KEY &&
			            GroupsByKey(*op.children[idx], join.duplicate_eliminated_columns, partition, delims)
			        ? SplitKind::SPLIT_BY_KEY
			        : SplitKind::REPLICATED;
			for (auto &source : delims) {
				if (source.join.get() == &join) {
					source.kind = kind;
				}
			}
		}
	}
	bool any_split = false;
	for (const auto &kind : children) {
		any_split = any_split || IsSplit(kind);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		auto source = FindDelimSource(delims, op.Cast<LogicalDelimGet>().table_index);
		return source && source->join ? source->kind : SplitKind::INVALID;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (rowid_scan.IsValid() && get.table_index == rowid_scan.GetIndex()) {
			return SplitKind::SPLIT;
		}
		for (const auto &scan : partition.scans) {
			if (scan.table_index == get.table_index) {
				return SplitKind::SPLIT_BY_KEY;
			}
		}
		return SplitKind::REPLICATED;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return children[0];
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		if (!any_split) {
			return SplitKind::REPLICATED;
		}
		return children[0] == SplitKind::SPLIT_BY_KEY && aggregate.grouping_sets.size() <= 1 &&
		               GroupsByKey(*op.children[0], aggregate.groups, partition, delims)
		           ? SplitKind::SPLIT_BY_KEY
		           : SplitKind::INVALID;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT:
		if (!any_split) {
			return SplitKind::REPLICATED;
		}
		return children[0] == SplitKind::SPLIT_BY_KEY &&
		               GroupsByKey(*op.children[0], op.Cast<LogicalDistinct>().distinct_targets, partition, delims)
		           ? SplitKind::SPLIT_BY_KEY
		           : SplitKind::INVALID;
	case LogicalOperatorType::LOGICAL_WINDOW:
		// Rows of a window partition can arrive in another order in each task, so even a replicated input may differ.
		return SplitKind::INVALID;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		const auto left = children[0];
		const auto right = children[1];
		if (!any_split) {
			return SplitKind::REPLICATED;
		}
		const auto join_type =
		    op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT ? JoinType::INNER : op.Cast<LogicalJoin>().join_type;
		// Both inputs split by the same key ranges: rows with equal keys meet in one task, whatever the Join type.
		const bool co_split = left == SplitKind::SPLIT_BY_KEY && right == SplitKind::SPLIT_BY_KEY &&
		                      (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		                       op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
		                      JoinsOnKey(op.Cast<LogicalComparisonJoin>(), partition, delims);
		switch (join_type) {
		case JoinType::INNER:
			if (co_split) {
				return SplitKind::SPLIT_BY_KEY;
			}
			return IsSplit(left) != IsSplit(right) ? (IsSplit(left) ? left : right) : SplitKind::INVALID;
		case JoinType::LEFT:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::SINGLE:
			// Each left row needs every row of the right input that it could match.
			if (co_split) {
				return SplitKind::SPLIT_BY_KEY;
			}
			return IsSplit(right) ? SplitKind::INVALID : left;
		case JoinType::MARK:
			// A NULL anywhere in the right input decides the mark of every left row without a match.
			return IsSplit(right) ? SplitKind::INVALID : left;
		case JoinType::RIGHT:
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
			if (co_split) {
				return SplitKind::SPLIT_BY_KEY;
			}
			return IsSplit(left) ? SplitKind::INVALID : right;
		case JoinType::OUTER:
			return co_split ? SplitKind::SPLIT_BY_KEY : SplitKind::INVALID;
		default:
			return SplitKind::INVALID;
		}
	}
	default:
		// Other operators are only known to give the same output in every task when their input does.
		return any_split ? SplitKind::INVALID : SplitKind::REPLICATED;
	}
}

} // namespace

SplitKind AnalyzeSplit(LogicalOperator &op, const KeyRangePartition &partition, optional_idx rowid_scan) {
	vector<DelimSource> delims;
	CollectDelimSources(op, delims);
	return Analyze(op, partition, rowid_scan, delims);
}

} // namespace duckdb
