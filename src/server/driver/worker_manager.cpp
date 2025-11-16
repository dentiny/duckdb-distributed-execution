#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "server/driver/worker_manager.hpp"
#include "utils/network_utils.hpp"

namespace duckdb {

void WorkerManager::RegisterWorker(const string &worker_id, const string &location) {
	std::lock_guard<std::mutex> lck(mu);
	auto &db_instance = *db.instance;

	auto worker_info = make_uniq<WorkerInfo>(worker_id, location);

	// Connect to the worker.
	auto status = worker_info->client->Connect();
	if (!status.ok()) {
		throw IOException("Failed to connect to worker %s at %s: %s", worker_id, location, status.ToString());
	}

	workers.emplace_back(std::move(worker_info));
	DUCKDB_LOG_DEBUG(db_instance, "Successfully registered worker '%s' at '%s'", worker_id, location);
}

vector<WorkerInfo *> WorkerManager::GetAvailableWorkers() {
	vector<WorkerInfo *> available;
	available.reserve(workers.size());

	std::lock_guard<std::mutex> lock(mu);
	for (auto &worker : workers) {
		available.emplace_back(worker.get());
	}
	return available;
}

idx_t WorkerManager::GetWorkerCount() const {
	return workers.size();
}

void WorkerManager::StartLocalWorkers(idx_t num_workers) {
	std::lock_guard<std::mutex> lock(mu);
	auto &db_instance = *db.instance;
	
	DUCKDB_LOG_DEBUG(db_instance, "Starting %llu local worker nodes", num_workers);
	
	for (idx_t idx = 0; idx < num_workers; ++idx) {
		// Use incrementing worker ID
		string worker_id = StringUtil::Format("worker_%llu", next_local_worker_id++);
		
		// Get an available port dynamically
		int port = GetAvailablePort(next_local_worker_port);
		if (port < 0) {
			throw IOException("Failed to find available port for worker %s", worker_id);
		}
		next_local_worker_port = port + 1; // Start searching from next port next time
		
		auto worker = make_uniq<WorkerNode>(worker_id, "localhost", port, &db);

		auto status = worker->Start();
		if (!status.ok()) {
			throw IOException("Failed to start worker %s: %s", worker_id, status.ToString());
		}

		string location = worker->GetLocation();
		
		// Register without lock since we already have it
		auto worker_info = make_uniq<WorkerInfo>(worker_id, location);
		auto connect_status = worker_info->client->Connect();
		if (!connect_status.ok()) {
			throw IOException("Failed to connect to worker %s at %s: %s", worker_id, location, connect_status.ToString());
		}
		workers.emplace_back(std::move(worker_info));
		
		local_workers.emplace_back(std::move(worker));
		DUCKDB_LOG_DEBUG(db_instance, "Started local worker '%s' at '%s'", worker_id, location);
	}
	
	DUCKDB_LOG_DEBUG(db_instance, "Successfully started %llu local workers (total workers: %llu)", 
	                 num_workers, workers.size());
}

} // namespace duckdb
