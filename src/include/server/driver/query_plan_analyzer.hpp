#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

// Forward declarations.
struct PlanPartitionInfo;

// Analyzes DuckDB logical/physical plans to extract information for distributed execution planning.
class QueryPlanAnalyzer {
public:
	explicit QueryPlanAnalyzer(Connection &conn_p);

	// Query DuckDB's estimated parallelization decision.
	// Returns the number of parallel tasks DuckDB would naturally create for this query.
	idx_t QueryEstimatedParallelism(LogicalOperator &logical_plan);

	// Extract partition information from physical plan.
	// Analyzes the plan structure to get hints about intelligent partitioning.
	PlanPartitionInfo ExtractPartitionInfo(LogicalOperator &logical_plan, idx_t num_workers);

	// Extract row group information for DuckDB-aligned partitioning.
	struct RowGroupPartitionInfo {
		// Which row groups to scan.
		vector<idx_t> row_group_ids;
		// Total row groups in table.
		idx_t total_row_groups = 0;
		// Approximate rows per row group (typically 122,880).
		idx_t rows_per_row_group = 0;
		// Whether row group info is available.
		bool valid = false;
	};
	RowGroupPartitionInfo ExtractRowGroupInfo(LogicalOperator &logical_plan);

	// Analyze query pipeline complexity.
	struct PipelineInfo {
		// Number of pipelines.
		idx_t pipeline_count = 0;
		// Whether pipelines have dependencies.
		bool has_dependencies = false;
		// Single pipeline, simple scan.
		bool is_simple_scan = true;
		// Has join operators.
		bool has_joins = false;
		// Has window, CTE, etc.
		bool has_complex_operators = false;
		// Types of each pipeline.
		vector<string> pipeline_types;
	};
	PipelineInfo AnalyzePipelines(LogicalOperator &logical_plan);

	// Merge strategy for distributed query results
	enum class MergeStrategy {
		CONCATENATE,     // Simple scans - just concatenate results
		AGGREGATE_MERGE, // Aggregations - need to merge partial aggregates
		DISTINCT_MERGE,  // DISTINCT - need to eliminate duplicates
		GROUP_BY_MERGE   // GROUP BY - need to merge grouped results
	};

	// Analyze query for aggregations and grouping to determine merge strategy
	struct QueryAnalysis {
		MergeStrategy merge_strategy = MergeStrategy::CONCATENATE;
		bool has_aggregation = false;
		bool has_group_by = false;
		bool has_distinct = false;
		bool has_order_by = false;
		vector<string> aggregate_functions; // e.g., ["COUNT", "SUM", "AVG"]
		vector<string> group_by_columns;    // Column names in GROUP BY
	};
	QueryAnalysis AnalyzeQuery(LogicalOperator &logical_plan);

private:
	Connection &conn;
};

} // namespace duckdb
