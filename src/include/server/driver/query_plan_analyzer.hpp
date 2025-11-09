#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

// Forward declarations
struct PlanPartitionInfo;

//! QueryPlanAnalyzer: Analyzes DuckDB logical/physical plans to extract
//! information for distributed execution planning
class QueryPlanAnalyzer {
public:
	explicit QueryPlanAnalyzer(Connection &conn);

	//! Query DuckDB's natural parallelization decision
	//! Returns the number of parallel tasks DuckDB would naturally create for this query
	idx_t QueryNaturalParallelism(LogicalOperator &logical_plan);

	//! Extract partition information from physical plan
	//! Analyzes the plan structure to get hints about intelligent partitioning
	PlanPartitionInfo ExtractPartitionInfo(LogicalOperator &logical_plan, idx_t num_workers);

	//! Extract row group information for DuckDB-aligned partitioning
	struct RowGroupPartitionInfo {
		vector<idx_t> row_group_ids; // Which row groups to scan
		idx_t total_row_groups;      // Total row groups in table
		idx_t rows_per_row_group;    // Approximate rows per row group (typically 122,880)
		bool valid;                  // Whether row group info is available

		RowGroupPartitionInfo() : total_row_groups(0), rows_per_row_group(0), valid(false) {
		}
	};

	RowGroupPartitionInfo ExtractRowGroupInfo(LogicalOperator &logical_plan);

	//! Analyze query pipeline complexity
	struct PipelineInfo {
		idx_t pipeline_count;          // Number of pipelines
		bool has_dependencies;         // Whether pipelines have dependencies
		bool is_simple_scan;           // Single pipeline, simple scan
		bool has_joins;                // Has join operators
		bool has_complex_operators;    // Has window, CTE, etc.
		vector<string> pipeline_types; // Types of each pipeline

		PipelineInfo()
		    : pipeline_count(0), has_dependencies(false), is_simple_scan(true), has_joins(false),
		      has_complex_operators(false) {
		}
	};

	PipelineInfo AnalyzePipelines(LogicalOperator &logical_plan);

	//! Merge strategy for distributed query results
	enum class MergeStrategy {
		CONCATENATE,     // Simple scans - just concatenate results
		AGGREGATE_MERGE, // Aggregations - need to merge partial aggregates
		DISTINCT_MERGE,  // DISTINCT - need to eliminate duplicates
		GROUP_BY_MERGE   // GROUP BY - need to merge grouped results
	};

	//! Analyze query for aggregations and grouping to determine merge strategy
	struct QueryAnalysis {
		MergeStrategy merge_strategy;
		bool has_aggregation;
		bool has_group_by;
		bool has_distinct;
		bool has_order_by;
		vector<string> aggregate_functions; // e.g., ["COUNT", "SUM", "AVG"]
		vector<string> group_by_columns;    // Column names in GROUP BY

		QueryAnalysis()
		    : merge_strategy(MergeStrategy::CONCATENATE), has_aggregation(false), has_group_by(false),
		      has_distinct(false), has_order_by(false) {
		}
	};

	QueryAnalysis AnalyzeQuery(LogicalOperator &logical_plan);

	//! Check if a logical plan contains only supported operators
	bool IsSupportedPlan(LogicalOperator &op);

private:
	Connection &conn;
};

} // namespace duckdb
