#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/query_result.hpp"
#include "server/driver/worker_manager.hpp"

#include <arrow/record_batch.h>

namespace duckdb {

// Simple distributed executor that partitions data and sends to workers
class DistributedExecutor {
public:
	explicit DistributedExecutor(WorkerManager &worker_manager, Connection &conn);

	// Execute a query in distributed manner
	// Returns nullptr if query cannot be distributed (falls back to local)
	unique_ptr<QueryResult> ExecuteDistributed(const string &sql);

private:
	// Check if query can be distributed (simple heuristic)
	bool CanDistribute(const string &sql);

	// Extract table name from SQL query
	string ExtractTableName(const string &sql);

	// Partition data across N workers (round-robin)
	vector<vector<unique_ptr<DataChunk>>> PartitionData(QueryResult &result, idx_t num_partitions);

	// Serialize partition to Arrow IPC format
	string SerializePartitionToArrowIPC(vector<unique_ptr<DataChunk>> &partition, const vector<LogicalType> &types,
	                                    const vector<string> &names);

	// Collect and merge results from worker streams
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

	// Partition table data and send to workers (deprecated)
	arrow::Status PartitionAndDistribute(const string &sql, vector<std::shared_ptr<arrow::RecordBatch>> &all_batches);

	// Collect results from workers and merge (deprecated)
	unique_ptr<QueryResult> CollectAndMerge(const vector<std::shared_ptr<arrow::RecordBatch>> &batches);

	WorkerManager &worker_manager;
	Connection &local_conn;
};

} // namespace duckdb
