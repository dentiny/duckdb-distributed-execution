#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "server/worker_node.hpp"
#include <memory>
#include <mutex>

namespace duckdb {

struct WorkerInfo {
	string worker_id;
	string location;
	std::unique_ptr<WorkerNodeClient> client;
	bool available;

	WorkerInfo(string id, string loc) : worker_id(std::move(id)), location(std::move(loc)), available(true) {
		client = std::make_unique<WorkerNodeClient>(location);
	}
};

// Manages a pool of worker nodes
class WorkerManager {
public:
	WorkerManager() = default;

	// Register a worker node
	void RegisterWorker(const string &worker_id, const string &location);

	// Get all available workers
	vector<WorkerInfo *> GetAvailableWorkers();

	// Get number of workers
	idx_t GetWorkerCount() const;

	// Start N local worker nodes for testing
	void StartLocalWorkers(idx_t num_workers);

private:
	vector<std::unique_ptr<WorkerInfo>> workers;
	vector<std::unique_ptr<WorkerNode>> local_workers; // For testing
	mutable std::mutex mutex;
};

} // namespace duckdb
