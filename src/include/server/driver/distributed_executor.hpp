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

	string SerializeLogicalPlan(LogicalOperator &op);
	string SerializeLogicalType(const LogicalType &type);

	// Collect and merge results from worker streams.
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

	WorkerManager &worker_manager;
	Connection &conn;
};

} // namespace duckdb
