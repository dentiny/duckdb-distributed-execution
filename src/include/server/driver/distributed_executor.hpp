#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "storage_config.pb.h"
#include "worker.pb.h"

#include <memory>

namespace arrow {
namespace flight {
class FlightStreamReader;
} // namespace flight
} // namespace arrow

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
	// streams. The exchange reads of the plans scan sources. Returns false if any task cannot start on a worker;
	// merging the others would drop that task's rows.
	bool ExecutePlans(const vector<string> &plans, const vector<distributed::ExchangeSource> &sources,
	                  vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams);
	// Runs plans as the producers of a new exchange with bucket_count buckets and waits until all have sealed. Sets
	// source even if one fails, so the caller can release the others' buckets. Returns false if a producer fails.
	bool ExecuteProducers(const vector<string> &plans, idx_t bucket_count, distributed::ExchangeSource &source);
	// Drops the buckets of source on its producers. Failures are only logged.
	void ReleaseExchange(const distributed::ExchangeSource &source);

private:
	// Sends requests[i] to the i-th available worker and stores the stream of its result in streams. Returns false if
	// there are fewer workers than requests or a task cannot start. Sets locations, if given, to the workers' locations
	// before sending any request.
	bool Dispatch(const vector<distributed::ExecutePartitionRequest> &requests,
	              vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	              optional_ptr<vector<string>> locations = nullptr);
	distributed::ExecutePartitionRequest MakeRequest(const string &plan) const;

	WorkerManager &worker_manager;
	Connection &conn;
	distributed::StorageConfig storage_config;
};

} // namespace duckdb
