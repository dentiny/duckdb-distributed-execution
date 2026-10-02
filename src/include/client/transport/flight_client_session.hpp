#pragma once

#include "client.pb.h"
#include "distributed.pb.h"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "storage_config.pb.h"
#include "utils/mutex.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <condition_variable>
#include <memory>
#include <thread>

namespace duckdb {

// A client registration with the control node. Registers on connect, renews the registration's lease with
// heartbeats, and sends single RPC attempts on behalf of the registered client.
class FlightClientSession {
public:
	FlightClientSession(string server_url, distributed::ClientRole role, distributed::StorageConfig storage_config);
	~FlightClientSession();

	arrow::Status Connect();
	// Stop heartbeats and unregister. Never throws, since it runs on destruction and DETACH.
	void Close();

	// Send one action and wait for its response.
	arrow::Status SendAction(const distributed::DistributedRequest &req, distributed::DistributedResponse &resp);
	// Run one scan request and collect every result batch.
	arrow::Status DoGet(const distributed::DistributedRequest &req,
	                    vector<std::shared_ptr<arrow::RecordBatch>> &batches);
	// Insert one batch into `table_name`, identified by the transaction fields of `identity`.
	arrow::Status DoPut(const string &table_name, const distributed::DistributedRequest &identity,
	                    const std::shared_ptr<arrow::RecordBatch> &batch, distributed::DistributedResponse &response);

private:
	arrow::Status RegisterClient();
	void UnregisterClientNoThrow();
	void HeartbeatLoop();

	string server_url;
	distributed::ClientRole role;
	distributed::StorageConfig storage_config;
	string client_id;
	arrow::flight::Location location;
	std::unique_ptr<arrow::flight::FlightClient> client;
	atomic<bool> stop_heartbeat {false};
	concurrency::mutex heartbeat_mutex;
	std::condition_variable heartbeat_cv DUCKDB_GUARDED_BY(heartbeat_mutex);
	std::thread heartbeat_thread;
};

} // namespace duckdb
