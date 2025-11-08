#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/query_result.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>

namespace duckdb {

// Forward declaration.
class Connection;
class WorkerManager;
class LogicalOperator;
class PhysicalOperator;

// STEP 2: Structure to hold partition information extracted from physical plan
struct PlanPartitionInfo {
	// The type of physical operator
	PhysicalOperatorType operator_type;
	
	// Estimated cardinality (row count)
	idx_t estimated_cardinality;
	
	// Natural parallelism (from EstimatedThreadCount)
	idx_t natural_parallelism;
	
	// Whether we can use intelligent partitioning for this plan
	bool supports_intelligent_partitioning;
	
	// For table scans: estimated rows per partition
	idx_t rows_per_partition;
	
	PlanPartitionInfo() 
		: operator_type(PhysicalOperatorType::INVALID),
		  estimated_cardinality(0),
		  natural_parallelism(0),
		  supports_intelligent_partitioning(false),
		  rows_per_partition(0) {
	}
};

// STEP 1 (Pipeline Tasks): Structure representing a distributed pipeline task
// This represents one unit of work that will be executed on a worker
struct DistributedPipelineTask {
	// Unique task identifier
	idx_t task_id;
	
	// Total number of parallel tasks
	idx_t total_tasks;
	
	// The SQL query to execute (for now, we'll start with SQL-based approach)
	// Future: serialize actual Pipeline structure
	string task_sql;
	
	// Task-specific metadata
	idx_t row_group_start;  // Starting row group for this task
	idx_t row_group_end;    // Ending row group for this task
	
	DistributedPipelineTask()
		: task_id(0), total_tasks(0), row_group_start(0), row_group_end(0) {
	}
};

// Simple distributed executor that partitions data and sends to workers
class DistributedExecutor {
public:
	DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p);

	// Execute a query in distributed manner.
	// Returns nullptr if query cannot be distributed, which will fall back to local execution.
	unique_ptr<QueryResult> ExecuteDistributed(const string &sql);

private:
	// Check if query can be distributed.
	// TODO(hjiang): currently it's purely heuristic SELECT query.
	bool CanDistribute(const string &sql);

	// Create per-partition SQL statement.
	// STEP 6: Now uses PlanPartitionInfo for intelligent partitioning
	string CreatePartitionSQL(const string &sql, idx_t partition_id, idx_t total_partitions,
	                         const PlanPartitionInfo &partition_info);

	// Check whether that the logical plan only contains operators we can currently distribute.
	// TODO(hjiang): Currently we only support limited operator based on heuristics.
	bool IsSupportedPlan(LogicalOperator &op);

	// STEP 1: Query DuckDB's natural parallelization decision
	// Returns the number of parallel tasks DuckDB would naturally create for this query
	idx_t QueryNaturalParallelism(LogicalOperator &logical_plan);

	// STEP 2: Extract partition information from physical plan
	// Analyzes the plan structure to get hints about intelligent partitioning
	PlanPartitionInfo ExtractPartitionInfo(LogicalOperator &logical_plan, idx_t num_workers);

	// STEP 1 (Pipeline Tasks): Extract distributed pipeline tasks from physical plan
	// This queries DuckDB's natural parallelization and creates task assignments
	vector<DistributedPipelineTask> ExtractPipelineTasks(LogicalOperator &logical_plan, 
	                                                      const string &base_sql,
	                                                      idx_t num_workers);

	// STEP 4: Query analysis for smart merging
	enum class MergeStrategy {
		CONCATENATE,      // Simple scans - just concatenate results
		AGGREGATE_MERGE,  // Aggregations - need to merge partial aggregates
		DISTINCT_MERGE,   // DISTINCT - need to eliminate duplicates
		GROUP_BY_MERGE    // GROUP BY - need to merge grouped results
	};
	
	struct QueryAnalysis {
		MergeStrategy merge_strategy;
		bool has_aggregates;
		bool has_group_by;
		bool has_distinct;
		vector<string> aggregate_functions;  // e.g., "count", "sum", "avg"
		vector<string> group_by_columns;
		
		QueryAnalysis() 
			: merge_strategy(MergeStrategy::CONCATENATE),
			  has_aggregates(false),
			  has_group_by(false),
			  has_distinct(false) {
		}
	};
	
	QueryAnalysis AnalyzeQuery(LogicalOperator &logical_plan);
	
	// STEP 4B: Helper methods for building merge SQL
	string BuildAggregateMergeSQL(const string &temp_table, const vector<string> &column_names, const QueryAnalysis &analysis);
	string BuildGroupByMergeSQL(const string &temp_table, const vector<string> &column_names, const QueryAnalysis &analysis);

	string SerializeLogicalPlan(LogicalOperator &op);
	string SerializeLogicalType(const LogicalType &type);

	// Collect and merge results from worker streams.
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);
	
	// STEP 4: Smart result merging with aggregation support
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types,
	                                               const QueryAnalysis &query_analysis);

	WorkerManager &worker_manager;
	Connection &conn;
};

} // namespace duckdb
