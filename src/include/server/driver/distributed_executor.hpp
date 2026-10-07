#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/query_result.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/result_merger.hpp"
#include "server/driver/task_partitioner.hpp"
#include "storage_config.pb.h"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <chrono>

namespace duckdb {

// Forward declaration.
class Connection;
class WorkerManager;
class LogicalOperator;
class SelectStatement;

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
	ROW_GROUP_ALIGNED, // Partitioned by DuckDB row groups
};

// Result structure containing query result and execution metadata.
struct DistributedExecutionResult {
	// Set when the query fails, e.g. a prepare error or a worker result schema mismatch.
	unique_ptr<QueryResult> result;
	// Set when workers executed the query; batches are forwarded to the client without decoding.
	std::shared_ptr<arrow::Schema> arrow_schema;
	vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches;
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

enum class DistributedFragmentKind { TABLE, PARTITIONED_JOIN };

// Distributed executor that partitions data and sends to workers.
class DistributedExecutor {
public:
	DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p,
	                    distributed::StorageConfig storage_config_p);

	// Returns an empty result when the query cannot be distributed.
	DistributedExecutionResult ExecuteDistributed(const string &sql, DistributedFragmentKind kind);
	bool CanPartitionJoin(LogicalOperator &plan, const SelectStatement &statement);

private:
	WorkerManager &worker_manager;
	Connection &conn;
	distributed::StorageConfig storage_config;

	unique_ptr<QueryPlanAnalyzer> plan_analyzer;
	unique_ptr<ResultMerger> result_merger;
	unique_ptr<TaskPartitioner> task_partitioner;
};

} // namespace duckdb
