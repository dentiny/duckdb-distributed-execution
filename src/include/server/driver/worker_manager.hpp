#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "server/worker/worker_node.hpp"

#include <memory>
#include <mutex>

namespace duckdb {

struct WorkerInfo {
	string worker_id;
	// Grpc location.
	// For example, grpc://<host>:<port>.
	string location;
	std::unique_ptr<WorkerNodeClient> client;
	// TODO(hjiang): Add node availability status.

	WorkerInfo(string id, string loc) : worker_id(std::move(id)), location(std::move(loc)) {
		client = std::make_unique<WorkerNodeClient>(location);
	}
};

// Manages a pool of worker nodes.
class WorkerManager {
public:
	explicit WorkerManager(DuckDB &db_ref) : db(db_ref) {
	}

	// Register a single external worker node.
	// The worker should already be running at the specified location.
	// Throws IOException if connection fails.
	void RegisterWorker(const string &worker_id, const string &location);

	// Get all available workers.
	vector<WorkerInfo *> GetAvailableWorkers();

	// Get number of workers.
	idx_t GetWorkerCount() const;

	// Start N local worker nodes (for testing/development).
	// Workers are started in background threads.
	void StartLocalWorkers(idx_t num_workers);

private:
	vector<std::unique_ptr<WorkerInfo>> workers;
	vector<std::unique_ptr<WorkerNode>> local_workers; // For testing
	mutable std::mutex mu;
	DuckDB &db;
	idx_t next_local_worker_id = 0;  // Track next worker ID for local workers
	int next_local_worker_port = 9000;  // Track next available port for local workers
};

} // namespace duckdb
