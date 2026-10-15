#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "utils/mutex.hpp"

#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>

namespace arrow {
namespace internal {
class ThreadPool;
} // namespace internal
} // namespace arrow

namespace duckdb {

class DuckDB;
class WorkerNode;
class WorkerNodeClient;

struct WorkerInfo {
	string worker_id;
	// Grpc location.
	// For example, grpc://<host>:<port>.
	string location;
	unique_ptr<WorkerNodeClient> client;
	// TODO(hjiang): Add node availability status.

	WorkerInfo(string id, string loc);
	~WorkerInfo();
};

// Manages a pool of worker nodes.
class WorkerManager {
public:
	explicit WorkerManager(DuckDB &db_ref);
	~WorkerManager();

	arrow::Result<std::shared_ptr<arrow::internal::ThreadPool>> GetDispatchPool(int32_t threads);

	// Register a single external worker node.
	arrow::Status RegisterWorker(const string &worker_id, const string &location);

	// Get all available workers.
	vector<WorkerInfo *> GetAvailableWorkers();

	// Get number of workers.
	idx_t GetWorkerCount() const;

	// Start a number of local worker nodes in background threads.
	// Only used for local testing and dev.
	arrow::Status StartLocalWorkers(idx_t num_workers);

private:
	mutable concurrency::mutex mu;
	vector<unique_ptr<WorkerInfo>> workers DUCKDB_GUARDED_BY(mu);
	DuckDB &db;
	std::shared_ptr<arrow::internal::ThreadPool> dispatch_pool DUCKDB_GUARDED_BY(mu);

	// Local workers used for local testing.
	vector<unique_ptr<WorkerNode>> local_workers DUCKDB_GUARDED_BY(mu);
	// Used to track next worker ID for local workers.
	idx_t next_local_worker_id DUCKDB_GUARDED_BY(mu) = 0;
	// Used to track next available port for local workers.
	int next_local_worker_port DUCKDB_GUARDED_BY(mu) = 9000;
};

} // namespace duckdb
