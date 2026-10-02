#pragma once

#include "distributed.pb.h"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "utils/mutex.hpp"

#include <chrono>

namespace duckdb {

// Enum for query execution modes based on partitioning strategy
enum class QueryExecutionMode {
	LOCAL,              // Local execution on driver (no distribution)
	DELEGATED,          // No partition - delegated to single worker node
	ROW_GROUP_PARTITION // Distributed with row-group-aligned partitioning
};

// Structure to store query execution information.
struct QueryExecutionInfo {
	string sql;                                                 // The SQL query
	QueryExecutionMode execution_mode;                          // Partitioning strategy used
	QueryPlanAnalyzer::MergeStrategy merge_strategy;            // How results were merged
	std::chrono::milliseconds query_duration;                   // Total query duration
	std::chrono::system_clock::time_point execution_start_time; // When query started (wall-clock time)
	idx_t num_workers_used = 0;                                 // Number of workers used
	idx_t num_tasks_generated = 0;                              // Number of tasks created

	QueryExecutionInfo()
	    : execution_mode(QueryExecutionMode::LOCAL), merge_strategy(QueryPlanAnalyzer::MergeStrategy::CONCATENATE),
	      query_duration(0), execution_start_time(std::chrono::system_clock::now()) {
	}
};

// Executions of every query run by the driver, reported to clients as query execution stats.
class QueryHistory {
public:
	void Record(QueryExecutionInfo info);
	vector<QueryExecutionInfo> Get() const;
	void Clear();
	// Fill `resp` with every recorded execution.
	void FillStatsResponse(distributed::DistributedResponse &resp) const;

private:
	mutable concurrency::mutex mutex;
	vector<QueryExecutionInfo> executions DUCKDB_GUARDED_BY(mutex);
};

} // namespace duckdb
