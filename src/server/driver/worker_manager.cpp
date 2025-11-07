#include "duckdb/common/string_util.hpp"
#include "server/driver/worker_manager.hpp"

namespace duckdb {

void WorkerManager::RegisterWorker(const string &worker_id, const string &location) {
	std::lock_guard<std::mutex> lock(mutex);

	auto worker_info = make_uniq<WorkerInfo>(worker_id, location);

	// Connect to the worker.
	auto status = worker_info->client->Connect();
	if (!status.ok()) {
		throw IOException("Failed to connect to worker %s at %s: %s", worker_id, location, status.ToString());
	}

	workers.push_back(std::move(worker_info));
}

vector<WorkerInfo *> WorkerManager::GetAvailableWorkers() {
	std::lock_guard<std::mutex> lock(mutex);

	vector<WorkerInfo *> available;
	for (auto &worker : workers) {
		if (worker->available) {
			available.push_back(worker.get());
		}
	}
	return available;
}

idx_t WorkerManager::GetWorkerCount() const {
	return workers.size();
}

void WorkerManager::StartLocalWorkers(idx_t num_workers) {
	constexpr int WORKER_BASE_PORT = 9000;
	for (idx_t idx = 0; idx < num_workers; ++idx) {
		string worker_id = StringUtil::Format("worker_%llu", idx);
		auto worker = make_uniq<WorkerNode>(worker_id, "localhost", WORKER_BASE_PORT + idx, &db);

		auto status = worker->Start();
		if (!status.ok()) {
			throw IOException("Failed to start worker %s: %s", worker_id, status.ToString());
		}

		string location = worker->GetLocation();
		RegisterWorker(worker_id, location);

		local_workers.emplace_back(std::move(worker));
	}
}

} // namespace duckdb
