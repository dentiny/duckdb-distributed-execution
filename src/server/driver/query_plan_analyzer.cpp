#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/distributed_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

QueryPlanAnalyzer::QueryPlanAnalyzer(Connection &conn_p) : conn(conn_p) {
}

bool QueryPlanAnalyzer::IsSupportedPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op.children.size() != 1) {
			return false;
		}
		return IsSupportedPlan(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_GET:
		return true;
	default:
		return false;
	}
}

idx_t QueryPlanAnalyzer::QueryNaturalParallelism(LogicalOperator &logical_plan) {
	auto &db_instance = *conn.context->db;
	idx_t estimated_threads = 0;

	try {
		// Wrap physical plan generation in a transaction (mimicking DuckDB's internal behavior)
		// This is necessary because physical plan generation requires an active transaction context
		conn.context->RunFunctionInTransaction([&]() {
			// Clone the logical plan since Plan() takes ownership
			auto cloned_plan = logical_plan.Copy(*conn.context);

			// Create a physical plan from the logical plan using the proper API
			PhysicalPlanGenerator generator(*conn.context);
			auto physical_plan = generator.Plan(std::move(cloned_plan));

			// Query the estimated thread count
			// This tells us how many parallel tasks DuckDB would naturally create
			estimated_threads = physical_plan->Root().EstimatedThreadCount();

			DUCKDB_LOG_DEBUG(db_instance,
			                 StringUtil::Format("📊 [PARALLELISM] DuckDB's natural parallelism decision:"));
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("   - Estimated thread count: %llu",
			                                                 static_cast<long long unsigned>(estimated_threads)));
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("   - Physical plan type: %s",
			                                                 PhysicalOperatorToString(physical_plan->Root().type)));
		});

		return estimated_threads;

	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("📊 [PARALLELISM] Failed to query natural parallelism: %s", ex.what()));
		return 0;
	}
}

PlanPartitionInfo QueryPlanAnalyzer::ExtractPartitionInfo(LogicalOperator &logical_plan, idx_t num_workers) {
	auto &db_instance = *conn.context->db;
	PlanPartitionInfo info;

	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("🔍 [PLAN] ExtractPartitionInfo logical=%s workers=%llu children=%llu",
	                                    LogicalOperatorToString(logical_plan.type),
	                                    static_cast<long long unsigned>(num_workers),
	                                    static_cast<long long unsigned>(logical_plan.children.size())));

	try {
		// Wrap physical plan generation in a transaction (mimicking DuckDB's internal behavior)
		conn.context->RunFunctionInTransaction([&]() {
			// Clone the logical plan since Plan() takes ownership
			auto cloned_plan = logical_plan.Copy(*conn.context);

			// Generate physical plan using the proper API
			PhysicalPlanGenerator generator(*conn.context);
			auto physical_plan_ptr = generator.Plan(std::move(cloned_plan));
			auto &physical_plan = physical_plan_ptr->Root();

			// Extract basic information
			info.operator_type = physical_plan.type;
			info.estimated_cardinality = physical_plan.estimated_cardinality;
			info.natural_parallelism = physical_plan.EstimatedThreadCount();

			DUCKDB_LOG_DEBUG(
			    db_instance,
			    StringUtil::Format("🔍 [PLAN ANALYSIS] operator=%s estimated_cardinality=%llu natural_parallelism=%llu",
			                       PhysicalOperatorToString(info.operator_type),
			                       static_cast<long long unsigned>(info.estimated_cardinality),
			                       static_cast<long long unsigned>(info.natural_parallelism)));

			// Analyze if we can use intelligent partitioning
			// For now, we support intelligent partitioning for:
			// 1. Table scans with sufficient cardinality
			// 2. Plans where natural parallelism matches or exceeds worker count
			if (info.estimated_cardinality > 0 && num_workers > 0) {
				info.rows_per_partition = (info.estimated_cardinality + num_workers - 1) / num_workers;

				// We can use intelligent partitioning if:
				// - It's a table scan (most common case)
				// - We have enough rows per partition (at least 100 rows per worker)
				if (info.operator_type == PhysicalOperatorType::TABLE_SCAN && info.rows_per_partition >= 100) {
					info.supports_intelligent_partitioning = true;

					DUCKDB_LOG_DEBUG(db_instance,
					                 StringUtil::Format(
					                     "✅ [PLAN ANALYSIS] Intelligent partitioning enabled rows_per_partition=%llu",
					                     static_cast<long long unsigned>(info.rows_per_partition)));
				} else {
					DUCKDB_LOG_DEBUG(db_instance,
					                 StringUtil::Format("ℹ️  [PLAN ANALYSIS] Using fallback partitioning reason=%s",
					                                    info.operator_type != PhysicalOperatorType::TABLE_SCAN
					                                        ? "not_table_scan"
					                                        : "insufficient_rows"));
				}
			} else {
				DUCKDB_LOG_DEBUG(db_instance,
				                 StringUtil::Format("ℹ️  [PLAN ANALYSIS] Cannot determine partitioning strategy"));
			}
		}); // End of RunFunctionInTransaction lambda

		return info;

	} catch (std::exception &ex) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("🔍 [PLAN ANALYSIS] Cannot generate physical plan: %s", ex.what()));
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("   - Using fallback partitioning strategy (rowid %%)"));

		// Return empty info, which triggers fallback partitioning
		return info;
	}
}

QueryPlanAnalyzer::RowGroupPartitionInfo QueryPlanAnalyzer::ExtractRowGroupInfo(LogicalOperator &logical_plan) {
	auto &db_instance = *conn.context->db;
	RowGroupPartitionInfo row_group_info;

	try {
		conn.context->RunFunctionInTransaction([&]() {
			// Generate physical plan
			auto cloned_plan = logical_plan.Copy(*conn.context);
			PhysicalPlanGenerator generator(*conn.context);
			auto physical_plan_ptr = generator.Plan(std::move(cloned_plan));
			auto &physical_plan = physical_plan_ptr->Root();

			// For now, use estimated cardinality and DEFAULT_ROW_GROUP_SIZE to infer row groups
			if (physical_plan.estimated_cardinality > 0) {
				// Calculate approximate number of row groups
				// DuckDB typically uses DEFAULT_ROW_GROUP_SIZE (usually 122880) rows per row group
				constexpr idx_t APPROX_ROW_GROUP_SIZE = 122880;

				row_group_info.total_row_groups =
				    (physical_plan.estimated_cardinality + APPROX_ROW_GROUP_SIZE - 1) / APPROX_ROW_GROUP_SIZE;
				row_group_info.rows_per_row_group = APPROX_ROW_GROUP_SIZE;

				// If we have at least one row group, mark as valid
				if (row_group_info.total_row_groups > 0) {
					row_group_info.valid = true;
					DUCKDB_LOG_DEBUG(
					    db_instance,
					    StringUtil::Format("🗂️  [STEP5] Row group info: %llu row groups, %llu rows/group",
					                       static_cast<long long unsigned>(row_group_info.total_row_groups),
					                       static_cast<long long unsigned>(row_group_info.rows_per_row_group)));
				}
			}
		});
	} catch (std::exception &ex) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("⚠️  [STEP5] Failed to extract row group info: %s", ex.what()));
	}

	return row_group_info;
}

QueryPlanAnalyzer::PipelineInfo QueryPlanAnalyzer::AnalyzePipelines(LogicalOperator &logical_plan) {
	auto &db_instance = *conn.context->db;
	PipelineInfo info;

	try {
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
					info.pipeline_types.push_back("JOIN");
					break;

				case PhysicalOperatorType::WINDOW:
				case PhysicalOperatorType::STREAMING_WINDOW:
					info.has_complex_operators = true;
					info.is_simple_scan = false;
					info.pipeline_types.push_back("WINDOW");
					break;

				case PhysicalOperatorType::RECURSIVE_CTE:
				case PhysicalOperatorType::CTE:
					info.has_complex_operators = true;
					info.is_simple_scan = false;
					info.pipeline_types.push_back("CTE");
					break;

				case PhysicalOperatorType::HASH_GROUP_BY:
				case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
					info.pipeline_types.push_back("HASH_AGG");
					break;

				case PhysicalOperatorType::ORDER_BY:
					info.pipeline_types.push_back("ORDER_BY");
					break;

				case PhysicalOperatorType::TABLE_SCAN:
					info.pipeline_types.push_back("TABLE_SCAN");
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

			DUCKDB_LOG_DEBUG(
			    db_instance,
			    StringUtil::Format("🔀 [STEP6] Pipeline analysis: %llu pipelines, dependencies=%d, simple=%d",
			                       static_cast<long long unsigned>(info.pipeline_count), info.has_dependencies,
			                       info.is_simple_scan));
		});
	} catch (std::exception &ex) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("⚠️  [STEP6] Failed to analyze pipelines: %s", ex.what()));
		// Default to assuming complex query
		info.pipeline_count = 1;
		info.is_simple_scan = false;
	}

	return info;
}

QueryPlanAnalyzer::QueryAnalysis QueryPlanAnalyzer::AnalyzeQuery(LogicalOperator &logical_plan) {
	QueryAnalysis analysis;
	auto &db_instance = *conn.context->db;

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

	DUCKDB_LOG_DEBUG(
	    db_instance,
	    StringUtil::Format(
	        "🔍 [QUERY ANALYSIS] merge_strategy=%d has_aggregation=%d has_group_by=%d has_distinct=%d has_order_by=%d",
	        static_cast<int>(analysis.merge_strategy), analysis.has_aggregation, analysis.has_group_by,
	        analysis.has_distinct, analysis.has_order_by));

	return analysis;
}

} // namespace duckdb
