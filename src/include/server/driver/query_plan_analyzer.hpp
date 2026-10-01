#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class SelectStatement;

// Analyzes DuckDB logical/physical plans to extract information for distributed execution planning.
class QueryPlanAnalyzer {
public:
	static constexpr const char *PARTIAL_TABLE_NAME = "__distributed_partial_results__";

	explicit QueryPlanAnalyzer(Connection &conn_p);

	// Extract row group information for DuckDB-aligned partitioning.
	struct RowGroupPartitionInfo {
		// Starting rowids of actual row groups, in storage order.
		vector<idx_t> row_group_starts;
		// Exclusive upper rowid bound, including deleted rows.
		idx_t rowid_end = 0;
		// Total row groups in table.
		idx_t total_row_groups = 0;
		// Whether row group info is available.
		bool valid = false;
	};
	RowGroupPartitionInfo ExtractRowGroupInfo(LogicalOperator &logical_plan);

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
		bool supports_partitioned_aggregation = false;
		string partial_sql;
		string final_sql;
	};
	QueryAnalysis AnalyzeQuery(LogicalOperator &logical_plan, const SelectStatement &statement);

private:
	Connection &conn;
};

} // namespace duckdb
