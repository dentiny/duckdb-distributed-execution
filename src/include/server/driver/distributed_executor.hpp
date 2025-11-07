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

// Simple distributed executor that partitions data and sends to workers
class DistributedExecutor {
public:
	DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p);

	// Execute a query in distributed manner
	// Returns nullptr if query cannot be distributed, which will fall back to local execution.
	unique_ptr<QueryResult> ExecuteDistributed(const string &sql);

private:
	// Check if query can be distributed.
	// TODO(hjiang): currently it's purely heuristic SELECT query.
	bool CanDistribute(const string &sql);

	// Create per-partition SQL statement.
	string CreatePartitionSQL(const string &sql, idx_t partition_id, idx_t total_partitions);

	// Validate that the logical plan only contains operators we can currently distribute.
	bool IsSupportedPlan(LogicalOperator &op);

	string SerializeLogicalPlan(LogicalOperator &op);
	string SerializeLogicalType(const LogicalType &type);

	// Collect and merge results from worker streams.
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

	WorkerManager &worker_manager;
	Connection &conn;
};

} // namespace duckdb
