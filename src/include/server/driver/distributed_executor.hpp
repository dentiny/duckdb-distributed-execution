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

	// Extract table name from SQL query.
	// TODO(hjiang): currently it's parsed out in string, it's better to get table name from table catalog entry.
	string ExtractTableName(const string &sql);

	// Partition data across N workers (round-robin).
	vector<vector<unique_ptr<DataChunk>>> PartitionData(QueryResult &result, idx_t num_partitions);

	// Serialize partition to Arrow IPC format.
	string SerializePartitionToArrowIPC(vector<unique_ptr<DataChunk>> &partition, const vector<LogicalType> &types,
	                                    const vector<string> &names);

	// Collect and merge results from worker streams.
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

	WorkerManager &worker_manager;
	Connection &conn;
};

} // namespace duckdb
