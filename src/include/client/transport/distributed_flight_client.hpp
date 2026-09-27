// Arrow Flight-based RPC client for distributed execution.

#pragma once

#include "distributed.pb.h"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_result.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <thread>

namespace duckdb {

class DatabaseInstance;

class DistributedFlightClient {
public:
	DistributedFlightClient(string server_url, distributed::ClientRole role_p,
	                        optional_ptr<DatabaseInstance> db_instance_p = nullptr);
	~DistributedFlightClient();

	// Connect to server.
	arrow::Status Connect();
	void Close();

	// Execute one complete non-query statement on the control node.
	arrow::Status ExecuteStatement(const string &sql, const string &client_catalog,
	                               distributed::DistributedResponse &response);

	// Apply a transaction lifecycle action to this client's server-side connection.
	arrow::Status ManageTransaction(distributed::TransactionAction action, distributed::DistributedResponse &response);

	// Load extension.
	arrow::Status LoadExtension(const string &extension_name, const string &repository, const string &version,
	                            distributed::DistributedResponse &response);

	// Check if table exists.
	arrow::Status TableExists(const string &table_name, bool &exists);

	// Insert data using Arrow RecordBatch.
	arrow::Status InsertData(const string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                         distributed::DistributedResponse &response);

	// Scan table and get Arrow Flight stream
	arrow::Status ScanTable(const string &table_name, uint64_t limit, uint64_t offset,
	                        vector<std::shared_ptr<arrow::RecordBatch>> &batches);

	// Get query execution statistics from the server
	arrow::Status GetQueryExecutionStats(distributed::DistributedResponse &response);

private:
	arrow::Status RegisterClient();
	void UnregisterClientNoThrow();
	void HeartbeatLoop();

	// RPC implementation to send request and block wait response.
	arrow::Status SendAction(const distributed::DistributedRequest &req, distributed::DistributedResponse &resp);
	// Assign the active transaction and one operation sequence, then replay that operation on transport failures.
	arrow::Status SendIdempotentAction(distributed::DistributedRequest &req, distributed::DistributedResponse &resp);

private:
	string server_url;
	distributed::ClientRole role;
	optional_ptr<DatabaseInstance> db_instance;
	string client_id;
	arrow::flight::Location location;
	std::unique_ptr<arrow::flight::FlightClient> client;
	atomic<bool> stop_heartbeat {false};
	mutex heartbeat_mutex;
	std::condition_variable heartbeat_cv;
	std::thread heartbeat_thread;
	// Serializes all transaction RPCs and retains stable identifiers needed to retry an ambiguous operation.
	mutex transaction_mutex;
	uint64_t transaction_id = 0;
	uint64_t next_transaction_id = 1;
	uint64_t next_request_sequence = 1;
	// A statement with an exhausted transport retry has an ambiguous outcome; only ROLLBACK may follow.
	bool transaction_requires_rollback = false;
	// A lifecycle action with a lost response is retried before a later BEGIN can allocate another transaction ID.
	distributed::TransactionAction pending_transaction_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
};

} // namespace duckdb
