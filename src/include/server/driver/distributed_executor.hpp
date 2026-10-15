#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "storage_config.pb.h"

#include <arrow/flight/client.h>

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
	// Starts each of plans, written by PlanSerializer, as one worker task and stores the stream of its result in
	// streams. Returns false if any task cannot start on a worker; merging the others would drop that task's rows.
	bool ExecutePlans(const vector<string> &plans, vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams);

private:
	WorkerManager &worker_manager;
	Connection &conn;
	distributed::StorageConfig storage_config;
};

} // namespace duckdb
