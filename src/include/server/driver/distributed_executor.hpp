#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "storage_config.pb.h"

#include <arrow/record_batch.h>

namespace duckdb {

// Forward declaration.
class Connection;
class WorkerManager;

// Runs serialized plan fragments on worker nodes.
class DistributedExecutor {
public:
	DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p,
	                    distributed::StorageConfig storage_config_p);

	idx_t NumWorkers();
	// Runs each of plans, written by PlanSerializer, as one worker task and stores its result in task_batches.
	// Returns false if any task cannot run on a worker; merging the others would drop that task's rows.
	bool ExecutePlans(const vector<string> &plans, vector<arrow::RecordBatchVector> &task_batches);

private:
	WorkerManager &worker_manager;
	Connection &conn;
	distributed::StorageConfig storage_config;
};

} // namespace duckdb
