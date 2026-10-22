#include "server/driver/distributed_executor.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
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
distributed::ExecutePartitionRequest DistributedExecutor::MakeRequest(const string &plan) const {
	distributed::ExecutePartitionRequest req;
	req.set_serialized_plan(plan);
	*req.mutable_storage_config() = storage_config;
	req.set_build_id(PlanSerializer::BuildId());
	return req;
}

bool DistributedExecutor::ExecutePlans(const vector<string> &plans, const vector<distributed::ExchangeSource> &sources,
                                       vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams) {
	vector<distributed::ExecutePartitionRequest> requests;
	requests.reserve(plans.size());
	for (const auto &plan : plans) {
		requests.emplace_back(MakeRequest(plan));
		for (const auto &source : sources) {
			*requests.back().add_exchange_sources() = source;
		}
	}
	return Dispatch(requests, streams);
}

bool DistributedExecutor::ExecuteProducers(const vector<string> &plans, idx_t bucket_count,
                                           distributed::ExchangeSource &source) {
	source.set_exchange_id(UUID::ToString(UUID::GenerateRandomUUID()));
	vector<distributed::ExecutePartitionRequest> requests;
	requests.reserve(plans.size());
	for (const auto &plan : plans) {
		requests.emplace_back(MakeRequest(plan));
		auto &write = *requests.back().mutable_exchange_write();
		write.set_exchange_id(source.exchange_id());
		write.set_bucket_count(NumericCast<uint32_t>(bucket_count));
	}
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> streams;
	// A producer that fails after it starts keeps an unsealed buffer, so every worker sent one is a producer, also if
	// the dispatch throws.
	vector<string> producers;
	bool started;
	try {
		started = Dispatch(requests, streams, producers);
	} catch (...) {
		*source.mutable_producers() = {producers.begin(), producers.end()};
		throw;
	}
	*source.mutable_producers() = {producers.begin(), producers.end()};
	if (!started) {
		return false;
	}
	// A producer returns its row counts once it has sealed its buckets.
	for (auto &stream : streams) {
		auto counts = stream->ToTable();
		if (!counts.ok()) {
			DUCKDB_LOG_WARNING(*conn.context->db,
			                   StringUtil::Format("Exchange producer failed: %s", counts.status().ToString()));
			return false;
		}
	}
	return true;
}

void DistributedExecutor::ReleaseExchange(const distributed::ExchangeSource &source) {
	// A producer that has left the workers took its buckets with it.
	for (auto *worker : worker_manager.GetAvailableWorkers()) {
		if (std::find(source.producers().begin(), source.producers().end(), worker->location) ==
		    source.producers().end()) {
			continue;
		}
		auto status = worker->client->ReleaseExchange(source.exchange_id());
		if (!status.ok()) {
			DUCKDB_LOG_WARNING(*conn.context->db,
			                   StringUtil::Format("Releasing exchange %s on %s failed: %s", source.exchange_id(),
			                                      worker->location, status.ToString()));
		}
	}
}

bool DistributedExecutor::Dispatch(const vector<distributed::ExecutePartitionRequest> &requests,
                                   vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                   optional_ptr<vector<string>> locations) {
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
	if (requests.size() > workers.size()) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("%llu tasks but %llu workers, falling back to local execution",
		                                                 requests.size(), workers.size()));
		return false;
	}
	if (locations) {
		locations->clear();
		for (idx_t task_idx = 0; task_idx < requests.size(); ++task_idx) {
			locations->emplace_back(workers[task_idx]->location);
		}
	}
	streams.clear();
	streams.resize(requests.size());
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
