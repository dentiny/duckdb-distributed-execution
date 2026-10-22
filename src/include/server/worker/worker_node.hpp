#pragma once

#include "distributed.pb.h"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/database.hpp"
#include "server/object_storage_database.hpp"
#include "server/worker/exchange_buffer.hpp"
#include "utils/mutex.hpp"

#include <arrow/flight/server.h>
#include <memory>

namespace arrow {
class RecordBatchReader;
} // namespace arrow

namespace duckdb {

class Connection;
class QueryResult;

// Simple worker node that executes queries on partitioned data.
class WorkerNode : public arrow::flight::FlightServerBase {
public:
	explicit WorkerNode(string worker_id_p, string host_p = "0.0.0.0", int port_p = 0);
	~WorkerNode() override = default;

	arrow::Status Start();
	void Shutdown();
	string GetLocation() const;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

private:
	arrow::Status HandleExecutePartition(const distributed::ExecutePartitionRequest &req,
	                                     std::shared_ptr<arrow::RecordBatchReader> &reader);
	// Run the task's serialized plan fragment on task_conn, which must outlive the result.
	arrow::Status ExecutePlanFragment(const distributed::ExecutePartitionRequest &req, Connection &task_conn,
	                                  unique_ptr<QueryResult> &result);
	// Adds the rows of result to the buckets of the request's exchange and returns the rows added per bucket.
	arrow::Status WriteExchange(const distributed::ExecutePartitionRequest &req, ObjectStorageDatabase &database,
	                            QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader);
	arrow::Status HandleReadExchange(const distributed::ReadExchangeRequest &req,
	                                 std::shared_ptr<arrow::RecordBatchReader> &reader);

	// Return this worker's read-only instance for the configuration, attaching it on first use.
	// The instance is a snapshot from that first attach; later writes are not visible through it.
	arrow::Result<ObjectStorageDatabase *> GetOrOpenObjectStorageDatabase(const distributed::StorageConfig &config);

	string worker_id;
	string host;
	int port;
	unique_ptr<DuckDB> db;

	concurrency::mutex object_storage_mutex;
	// Keyed by storage identity without credentials. Instances stay attached for the worker's lifetime.
	//
	// TODO(hjiang): Evict an instance when the final client detaches. Worker-lifetime caches retain resources, and
	// their read-only snapshots do not see later writes.
	unordered_map<string, unique_ptr<ObjectStorageDatabase>>
	    object_storage_databases DUCKDB_GUARDED_BY(object_storage_mutex);

	concurrency::mutex exchange_mutex;
	// Keyed by exchange ID. Declared after the databases, whose buffer managers hold the rows, so it is destroyed
	// first.
	unordered_map<string, shared_ptr<ExchangeBuffer>> exchanges DUCKDB_GUARDED_BY(exchange_mutex);
};

} // namespace duckdb
