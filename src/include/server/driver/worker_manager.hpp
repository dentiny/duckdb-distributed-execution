#pragma once

#include "duckdb.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "server/driver/worker_node_client.hpp"
#include "server/worker/worker_node.hpp"

#include <memory>
#include <mutex>

namespace duckdb {

struct WorkerInfo {
	string worker_id;
	// Grpc location.
	// For example, grpc://<host>:<port>.
	string location;
	unique_ptr<WorkerNodeClient> client;
	// TODO(hjiang): Add node availability status.

	WorkerInfo(string id, string loc) : worker_id(std::move(id)), location(std::move(loc)) {
		client = make_uniq<WorkerNodeClient>(location);
	}
};

// Manages a pool of worker nodes.
class WorkerManager {
public:
	explicit WorkerManager(DuckDB &db_ref) : db(db_ref) {
	}

	// Register a single external worker node.
	void RegisterWorker(const string &worker_id, const string &location);

	// Register or replace the driver node.
	// Unlike workers, only one driver node can be registered at a time.
	void RegisterOrReplaceDriver(const string &driver_id, const string &location);

	// Get all available workers.
	vector<WorkerInfo *> GetAvailableWorkers();

	// Get number of workers.
	idx_t GetWorkerCount() const;

	// Start a number of local worker nodes in background threads.
	// Only used for local testing and dev.
	void StartLocalWorkers(idx_t num_workers);

private:
	vector<std::unique_ptr<WorkerInfo>> workers;
	mutable std::mutex mu;
	DuckDB &db;

	// Driver node.
	unique_ptr<WorkerInfo> driver_node;

	// Local workers used for local testing.
	vector<unique_ptr<WorkerNode>> local_workers;
	// Used to track next worker ID for local workers.
	idx_t next_local_worker_id = 0;
	// Used to track next available port for local workers.
	int next_local_worker_port = 9000;
};

} // namespace duckdb
