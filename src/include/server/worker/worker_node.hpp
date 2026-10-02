#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "server/object_storage_database.hpp"
#include "utils/mutex.hpp"

#include <arrow/flight/api.h>
#include <memory>

namespace duckdb {

// Simple worker node that executes queries on partitioned data.
class WorkerNode : public arrow::flight::FlightServerBase {
public:
	explicit WorkerNode(string worker_id_p, string host_p = "0.0.0.0", int port_p = 0);
	~WorkerNode() override = default;

	arrow::Status Start();
	void Shutdown();
	string GetLocation() const;
	string GetWorkerId() const {
		return worker_id;
	}
	int GetPort() const {
		return port;
	}

	// Flight RPC methods
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

private:
	arrow::Status HandleExecutePartition(const distributed::ExecutePartitionRequest &req,
	                                     distributed::DistributedResponse &resp,
	                                     std::shared_ptr<arrow::RecordBatchReader> &reader);
	arrow::Status ExecuteSerializedPlan(const distributed::ExecutePartitionRequest &req,
	                                    unique_ptr<QueryResult> &result);

	// Execute a pipeline task on task_conn, which must outlive the result.
	arrow::Status ExecutePipelineTask(const distributed::ExecutePartitionRequest &req, Connection &task_conn,
	                                  unique_ptr<QueryResult> &result);

	// Return this worker's read-only instance for the configuration, attaching it on first use.
	// The instance is a snapshot from that first attach; later writes are not visible through it.
	arrow::Result<ObjectStorageDatabase *> GetOrOpenObjectStorageDatabase(const distributed::StorageConfig &config);

	string worker_id;
	string host;
	int port;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> conn;

	concurrency::mutex object_storage_mutex;
	// Keyed by storage identity without credentials. Instances stay attached for the worker's lifetime.
	//
	// TODO(hjiang): Evict an instance when the final client detaches. Worker-lifetime caches retain resources, and
	// their read-only snapshots do not see later writes.
	unordered_map<string, unique_ptr<ObjectStorageDatabase>>
	    object_storage_databases DUCKDB_GUARDED_BY(object_storage_mutex);
};

} // namespace duckdb
