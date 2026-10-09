#include "server/driver/query_plan_analyzer.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

namespace {

// Rewrites a pushed aggregate, `SELECT <groups>, <aggregates> FROM <table> [WHERE ...] [GROUP BY 1, ..., k]` (see
// `distributed_aggregate_pushdown.cpp`), into partial aggregates per row group and the query merging them.
// Returns false for any other query.
bool BuildPartialAggregation(const SelectStatement &original, QueryPlanAnalyzer::QueryAnalysis &analysis) {
	auto copy = original.Copy();
	auto &statement = copy->Cast<SelectStatement>();
	if (statement.node->type != QueryNodeType::SELECT_NODE) {
		return false;
	}
	auto &select = statement.node->Cast<SelectNode>();
	const auto &groups = select.groups.group_expressions;
	if (select.having || !select.modifiers.empty() || select.groups.grouping_sets.size() > 1) {
		return false;
	}
	for (idx_t idx = 0; idx < groups.size(); ++idx) {
		if (groups[idx]->GetExpressionClass() != ExpressionClass::CONSTANT ||
		    groups[idx]->Cast<ConstantExpression>().value != Value::INTEGER(NumericCast<int32_t>(idx + 1))) {
			return false;
		}
	}

	vector<unique_ptr<ParsedExpression>> partial_list;
	vector<string> final_list;
	for (idx_t idx = 0; idx < select.select_list.size(); ++idx) {
		auto expr = std::move(select.select_list[idx]);
		const auto column = StringUtil::Format("__c%llu", idx);
		if (idx < groups.size()) {
			final_list.push_back(column);
		} else {
			if (expr->GetExpressionClass() != ExpressionClass::FUNCTION) {
				return false;
			}
			auto &function = expr->Cast<FunctionExpression>();
			const auto name = function.function_name;
			if (function.distinct || function.filter || (function.order_bys && !function.order_bys->orders.empty())) {
				return false;
			}
			if (name == "avg") {
				auto count = function.Copy();
				count->Cast<FunctionExpression>().function_name = "count";
				count->SetAlias(column + "_c");
				function.function_name = "sum";
				expr->SetAlias(column + "_s");
				partial_list.push_back(std::move(expr));
				partial_list.push_back(std::move(count));
				final_list.push_back(StringUtil::Format("sum(%s_s) / sum(%s_c)", column, column));
				continue;
			}
			if (name == "count_star" || name == "count" || name == "sum") {
				final_list.push_back(StringUtil::Format("sum(%s)", column));
			} else if (name == "min" || name == "max") {
				final_list.push_back(StringUtil::Format("%s(%s)", name, column));
			} else {
				return false;
			}
		}
		expr->SetAlias(column);
		partial_list.push_back(std::move(expr));
	}
	select.select_list = std::move(partial_list);
	analysis.partial_sql = statement.ToString();
	// Group columns are exactly the non-aggregate outputs.
	analysis.final_sql = StringUtil::Format("SELECT %s FROM %s GROUP BY ALL", StringUtil::Join(final_list, ", "),
	                                        QueryPlanAnalyzer::PARTIAL_TABLE_NAME);
	return true;
}

} // namespace

QueryPlanAnalyzer::QueryPlanAnalyzer(Connection &conn_p) : conn(conn_p) {
}

QueryPlanAnalyzer::RowGroupPartitionInfo QueryPlanAnalyzer::ExtractRowGroupInfo(LogicalGet &get) {
	RowGroupPartitionInfo row_group_info;
	conn.context->RunFunctionInTransaction([&]() {
		auto table = get.GetTable();
		if (!table || !table->IsDuckTable() || table->ColumnExists("rowid")) {
			return;
		}
		// Use actual storage boundaries instead of estimated cardinality and DEFAULT_ROW_GROUP_SIZE
		// to obtain row groups.
		auto &storage = table->GetStorage();
		auto row_groups = storage.GetRowGroupCollection()->GetRowGroups();
		for (auto segment = row_groups->GetRootSegment(); segment; segment = row_groups->GetNextSegment(*segment)) {
			row_group_info.row_group_starts.push_back(segment->GetRowStart());
			row_group_info.rowid_end = segment->GetRowEnd();
		}
		// Calculate the number of row groups from storage metadata.
		row_group_info.total_row_groups = row_group_info.row_group_starts.size();
		// Mark native storage metadata as valid, including an empty table with no row groups.
		row_group_info.valid = true;
	});

	return row_group_info;
}

QueryPlanAnalyzer::QueryAnalysis QueryPlanAnalyzer::AnalyzeQuery(LogicalOperator &logical_plan,
                                                                 const SelectStatement &statement) {
	QueryAnalysis analysis;
	bool numeric_avg = true;

	// Recursively walk the logical plan tree to find aggregates, GROUP BY, DISTINCT
	std::function<void(LogicalOperator &)> analyze_operator = [&](LogicalOperator &op) {
		// Check for AGGREGATE operator
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			analysis.has_aggregation = true;

			auto &agg_op = op.Cast<LogicalAggregate>();

			// Check if this is a GROUP BY aggregation
			if (!agg_op.groups.empty()) {
				analysis.has_group_by = true;
			}

			// Merging `avg` as `sum / count` only reproduces it for numeric inputs.
			for (const auto &expr : agg_op.expressions) {
				const auto &aggregate = expr->Cast<BoundAggregateExpression>();
				if (aggregate.function.name == "avg" && !aggregate.children[0]->return_type.IsNumeric()) {
					numeric_avg = false;
				}
			}
		}

		// Check for DISTINCT operator
		if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
			analysis.has_distinct = true;
		}

		// Check for ORDER BY operator
		if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			analysis.has_order_by = true;
		}

		// Recursively analyze children
		for (auto &child : op.children) {
			analyze_operator(*child);
		}
	};

	// Start analysis from root
	analyze_operator(logical_plan);

	// Determine merge strategy based on what we found
	if (analysis.has_group_by) {
		analysis.merge_strategy = MergeStrategy::GROUP_BY_MERGE;
	} else if (analysis.has_aggregation) {
		analysis.merge_strategy = MergeStrategy::AGGREGATE_MERGE;
	} else if (analysis.has_distinct) {
		analysis.merge_strategy = MergeStrategy::DISTINCT_MERGE;
	} else {
		analysis.merge_strategy = MergeStrategy::CONCATENATE;
	}
	analysis.supports_partitioned_aggregation =
	    analysis.has_aggregation && numeric_avg && BuildPartialAggregation(statement, analysis);

	return analysis;
}

} // namespace duckdb
