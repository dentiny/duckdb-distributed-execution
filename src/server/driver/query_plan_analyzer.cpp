#include "server/driver/query_plan_analyzer.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

QueryPlanAnalyzer::QueryPlanAnalyzer(Connection &conn_p) : conn(conn_p) {
}

idx_t QueryPlanAnalyzer::QueryEstimatedParallelism(LogicalOperator &logical_plan) {
	idx_t estimated_threads = 0;

	// Wrap physical plan generation in a transaction (mimicking DuckDB's internal behavior).
	// This is necessary because physical plan generation requires an active transaction context.
	conn.context->RunFunctionInTransaction([&]() {
		auto cloned_logical_plan = logical_plan.Copy(*conn.context);
		PhysicalPlanGenerator generator(*conn.context);
		auto physical_plan = generator.Plan(std::move(cloned_logical_plan));

		// Query the estimated thread count.
		// This tells us how many parallel tasks DuckDB would naturally create.
		estimated_threads = physical_plan->Root().EstimatedThreadCount();
	});

	return estimated_threads;
}

QueryPlanAnalyzer::RowGroupPartitionInfo QueryPlanAnalyzer::ExtractRowGroupInfo(LogicalOperator &logical_plan) {
	RowGroupPartitionInfo row_group_info;

	conn.context->RunFunctionInTransaction([&]() {
		// Find the native table scan instead of generating a physical plan to estimate row groups.
		auto *op = &logical_plan;
		while (op->children.size() == 1) {
			op = op->children[0].get();
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto table = op->Cast<LogicalGet>().GetTable();
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

QueryPlanAnalyzer::PipelineInfo QueryPlanAnalyzer::AnalyzePipelines(LogicalOperator &logical_plan) {
	PipelineInfo info;

	conn.context->RunFunctionInTransaction([&]() {
		// Generate physical plan to analyze pipelines
		auto cloned_plan = logical_plan.Copy(*conn.context);
		PhysicalPlanGenerator generator(*conn.context);
		auto physical_plan_ptr = generator.Plan(std::move(cloned_plan));
		auto &physical_plan = physical_plan_ptr->Root();

		// Recursively analyze the physical plan structure
		std::function<void(PhysicalOperator &)> analyze_operator = [&](PhysicalOperator &op) {
			auto op_type = op.type;

			// Check for operators that typically create multiple pipelines
			switch (op_type) {
			case PhysicalOperatorType::HASH_JOIN:
			case PhysicalOperatorType::NESTED_LOOP_JOIN:
			case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
			case PhysicalOperatorType::CROSS_PRODUCT:
			case PhysicalOperatorType::IE_JOIN:
			case PhysicalOperatorType::ASOF_JOIN:
				info.has_joins = true;
				info.is_simple_scan = false;
				info.pipeline_types.emplace_back("JOIN");
				break;

			case PhysicalOperatorType::WINDOW:
			case PhysicalOperatorType::STREAMING_WINDOW:
				info.has_complex_operators = true;
				info.is_simple_scan = false;
				info.pipeline_types.emplace_back("WINDOW");
				break;

			case PhysicalOperatorType::RECURSIVE_CTE:
			case PhysicalOperatorType::CTE:
				info.has_complex_operators = true;
				info.is_simple_scan = false;
				info.pipeline_types.emplace_back("CTE");
				break;

			case PhysicalOperatorType::HASH_GROUP_BY:
			case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
				info.pipeline_types.emplace_back("HASH_AGG");
				break;

			case PhysicalOperatorType::ORDER_BY:
				info.pipeline_types.emplace_back("ORDER_BY");
				break;

			case PhysicalOperatorType::TABLE_SCAN:
				info.pipeline_types.emplace_back("TABLE_SCAN");
				break;

			default:
				break;
			}

			// Recurse into children
			for (auto &child : op.children) {
				analyze_operator(child.get());
			}
		};

		analyze_operator(physical_plan);

		// Estimate pipeline count based on operators found
		// Simple heuristic: each join/window/CTE creates additional pipelines
		info.pipeline_count = 1; // Base pipeline
		if (info.has_joins) {
			info.pipeline_count += 1; // Build + probe = 2 phases
			info.has_dependencies = true;
		}
		if (info.has_complex_operators) {
			info.pipeline_count += 1;
			info.has_dependencies = true;
		}
	});

	return info;
}

QueryPlanAnalyzer::QueryAnalysis QueryPlanAnalyzer::AnalyzeQuery(LogicalOperator &logical_plan) {
	QueryAnalysis analysis;

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

			// Extract aggregate function names
			for (auto &expr : agg_op.expressions) {
				if (expr->type == ExpressionType::BOUND_AGGREGATE) {
					auto &agg_expr = expr->Cast<BoundAggregateExpression>();
					analysis.aggregate_functions.push_back(agg_expr.function.name);
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

	return analysis;
}

} // namespace duckdb
