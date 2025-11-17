#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/query_result.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/result_merger.hpp"
#include "server/driver/task_partitioner.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <chrono>

namespace duckdb {

// Forward declaration.
class Connection;
class WorkerManager;
class LogicalOperator;
class PhysicalOperator;

// Struct to hold partition information extracted from physical plan.
struct PlanPartitionInfo {
	// The type of physical operator.
	PhysicalOperatorType operator_type = PhysicalOperatorType::INVALID;

	// Estimated cardinality.
	idx_t estimated_cardinality = 0;

	// Estimated parallelism from duckdb query plan analyzer.
	idx_t estimated_parallelism = 0;

	// Whether we can use intelligent partitioning for this plan.
	bool supports_intelligent_partitioning = false;

	// Used for table scans, which indicates estimated rows per partition.
	idx_t rows_per_partition = 0;
};

// Struct which represents a distributed pipeline task.
// This represents one unit of work that will be executed on a worker.
struct DistributedPipelineTask {
	// Unique task identifier.
	idx_t task_id = 0;

	// Total number of parallel tasks.
	idx_t total_tasks = 0;

	// The SQL query to execute (for now, we'll start with SQL-based approach).
	// Future: serialize actual Pipeline structure.
	string task_sql;

	// Task-specific metadata.
	// Starting row group for this task (inclusive).
	idx_t row_group_start = 0;
	// Ending row group for this task (inclusive).
	idx_t row_group_end = 0;
};

// Partitioning strategy used for distributed execution.
enum class PartitionStrategy {
	NONE,              // No partitioning (single task)
	NATURAL,           // Partitioned by natural parallelism (range or modulo based)
	ROW_GROUP_ALIGNED, // Partitioned by DuckDB row groups
};

// Result structure containing query result and execution metadata.
struct DistributedExecutionResult {
	unique_ptr<QueryResult> result;
	PartitionStrategy partition_strategy;
	QueryPlanAnalyzer::MergeStrategy merge_strategy;
	idx_t num_workers_used = 0;
	idx_t num_tasks = 0;
	std::chrono::milliseconds worker_execution_time;

	DistributedExecutionResult()
	    : merge_strategy(QueryPlanAnalyzer::MergeStrategy::CONCATENATE), partition_strategy(PartitionStrategy::NONE),
	      worker_execution_time(0) {
	}
};

// Distributed executor that partitions data and sends to workers.
class DistributedExecutor {
public:
	DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p);

	// Execute a query in distributed manner.
	// Returns result with nullptr if query cannot be distributed, which will fall back to local execution.
	DistributedExecutionResult ExecuteDistributed(const string &sql);

private:
	// Check if query can be distributed.
	bool CanDistribute(const string &sql);

	WorkerManager &worker_manager;
	Connection &conn;

	unique_ptr<QueryPlanAnalyzer> plan_analyzer;
	unique_ptr<PartitionSQLGenerator> sql_generator;
	unique_ptr<ResultMerger> result_merger;
	unique_ptr<TaskPartitioner> task_partitioner;
};

} // namespace duckdb
