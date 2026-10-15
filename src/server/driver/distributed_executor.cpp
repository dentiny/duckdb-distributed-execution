#include "server/driver/distributed_executor.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/driver/worker_node_client.hpp"

#include <arrow/flight/client.h>
#include <arrow/util/thread_pool.h>

namespace duckdb {

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p,
                                         distributed::StorageConfig storage_config_p)
    : worker_manager(worker_manager_p), conn(conn_p), storage_config(std::move(storage_config_p)) {
}

idx_t DistributedExecutor::NumWorkers() {
	return worker_manager.GetWorkerCount();
}

// Distributed execution mirrors DuckDB's parallel execution model: each worker task runs the plan fragment on its
// rows like a thread fills its LocalSinkState, and the driver combines the task results like the GlobalSinkState.
bool DistributedExecutor::ExecutePlans(const vector<string> &plans,
                                       vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams) {
	auto &db_instance = *conn.context->db;
	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return false;
	}
	if (storage_config.storage_case() == distributed::StorageConfig::STORAGE_NOT_SET) {
		DUCKDB_LOG_DEBUG(db_instance, "No storage configured for workers, falling back to local execution");
		return false;
	}
	// Each task runs on its own worker. Tasks planned for workers that have since left run locally instead.
	if (plans.size() > workers.size()) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("%llu tasks but %llu workers, falling back to local execution",
		                                                 plans.size(), workers.size()));
		return false;
	}
	vector<distributed::ExecutePartitionRequest> requests(plans.size());
	for (idx_t task_idx = 0; task_idx < plans.size(); ++task_idx) {
		auto &req = requests[task_idx];
		req.set_serialized_plan(plans[task_idx]);
		*req.mutable_storage_config() = storage_config;
		req.set_build_id(PlanSerializer::BuildId());
	}
	streams.clear();
	streams.resize(plans.size());
	Value thread_setting;
	if (!db_instance.TryGetCurrentSetting("duckherder_async_threads", thread_setting)) {
		throw InternalException("Duckherder async thread setting is not registered");
	}
	auto thread_count = thread_setting.GetValue<int64_t>();
	if (thread_count == 0) {
		const auto cpu_threads = TaskScheduler::GetScheduler(*conn.context).NumberOfThreads();
		thread_count = MinValue<idx_t>(cpu_threads, 64) * 4;
	}
	auto pool_result = worker_manager.GetDispatchPool(NumericCast<int>(thread_count));
	if (!pool_result.ok()) {
		throw IOException("Failed creating worker dispatch pool: %s", pool_result.status().ToString());
	}
	auto pool = *pool_result;
	vector<arrow::Future<>> dispatches;
	arrow::Status dispatch_error = arrow::Status::OK();
	dispatches.reserve(requests.size());
	for (idx_t task_idx = 0; task_idx < requests.size(); ++task_idx) {
		// DoGet blocks until the worker starts the task; keep the waits outside DuckDB's CPU-sized task pool.
		auto submitted = pool->Submit([&, task_idx]() -> arrow::Status {
			ARROW_ASSIGN_OR_RAISE(streams[task_idx], workers[task_idx]->client->ExecutePartition(requests[task_idx]));
			return arrow::Status::OK();
		});
		if (!submitted.ok()) {
			dispatch_error = submitted.status();
			break;
		}
		dispatches.emplace_back(*submitted);
	}
	// Complete every submitted task before returning or throwing: their captures
	// refer to this stack frame.
	for (auto &dispatch : dispatches) {
		dispatch.Wait();
	}
	auto cancel_streams = [&]() {
		for (auto &stream : streams) {
			if (stream) {
				stream->Cancel();
			}
		}
		streams.clear();
	};
	if (!dispatch_error.ok()) {
		cancel_streams();
		throw IOException("Worker dispatch failed: %s", dispatch_error.ToString());
	}
	for (idx_t task_idx = 0; task_idx < dispatches.size(); ++task_idx) {
		const auto &status = dispatches[task_idx].status();
		if (!status.ok()) {
			// Merging the remaining partitions would silently drop this task's rows.
			DUCKDB_LOG_WARNING(db_instance,
			                   StringUtil::Format("Worker %s failed starting task %llu, falling back to local "
			                                      "execution: %s",
			                                      workers[task_idx]->worker_id,
			                                      static_cast<long long unsigned>(task_idx), status.ToString()));
			cancel_streams();
			return false;
		}
	}
	return true;
}

} // namespace duckdb
