#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/query_result.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/result_merger.hpp"
#include "server/driver/task_partitioner.hpp"

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
	    : operator_type(PhysicalOperatorType::INVALID), estimated_cardinality(0), natural_parallelism(0),
	      supports_intelligent_partitioning(false), rows_per_partition(0) {
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
	idx_t row_group_start; // Starting row group for this task
	idx_t row_group_end;   // Ending row group for this task

	DistributedPipelineTask() : task_id(0), total_tasks(0), row_group_start(0), row_group_end(0) {
	}
};

// Distributed executor that partitions data and sends to workers
// Now uses extracted modules for plan analysis, SQL generation, task partitioning, and result merging
class DistributedExecutor {
public:
	DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p);

	// Execute a query in distributed manner.
	// Returns nullptr if query cannot be distributed, which will fall back to local execution.
	unique_ptr<QueryResult> ExecuteDistributed(const string &sql);

private:
	// Check if query can be distributed.
	bool CanDistribute(const string &sql);

	WorkerManager &worker_manager;
	Connection &conn;

	// Extracted modules
	unique_ptr<QueryPlanAnalyzer> plan_analyzer;
	unique_ptr<PartitionSQLGenerator> sql_generator;
	unique_ptr<ResultMerger> result_merger;
	unique_ptr<TaskPartitioner> task_partitioner;
};

} // namespace duckdb
