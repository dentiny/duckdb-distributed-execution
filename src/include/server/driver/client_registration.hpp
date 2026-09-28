#pragma once

#include "client.pb.h"
#include "transaction.pb.h"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "transaction_constants.hpp"
#include "utils/mutex.hpp"

#include <memory>

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace duckdb {

class Connection;
class DistributedExecutor;
class DuckDB;
class WorkerManager;

enum class ClientRequestTransport : uint8_t { NONE, ACTION, DO_GET, DO_PUT };

// Owns the Control Node resources and bounded transaction replay state for one registered client.
struct ClientRegistration {
	ClientRegistration(DuckDB &db, WorkerManager &worker_manager, distributed::ClientRole role_p);
	~ClientRegistration();

	distributed::ClientRole role;
	// Last authorized request time in steady-clock milliseconds, updated concurrently by RPC handlers.
	atomic<int64_t> last_seen;
	// DuckDB connections are session-scoped and must not execute concurrent requests.
	concurrency::mutex connection_mutex;
	// Dedicated DuckDB session for this registered client.
	unique_ptr<Connection> connection;
	// Distributed execution components bound to this client's DuckDB session.
	unique_ptr<DistributedExecutor> distributed_executor;
	// One connection has at most one active transaction. The finished watermark and its outcome make retries
	// idempotent with constant memory; older outcomes no longer need to be replayed after a newer transaction
	// starts.
	uint64_t active_transaction_id = INVALID_TRANSACTION_ID;
	uint64_t finished_transaction_id = INVALID_TRANSACTION_ID;
	distributed::TransactionStatus finished_transaction_status = distributed::TRANSACTION_STATUS_UNKNOWN;
	// Only the latest request in an active transaction is retained. A retry with the same sequence replays this
	// result, while a newer sequence replaces it, keeping replay memory bounded apart from the latest query result.
	uint64_t last_request_sequence = INVALID_REQUEST_SEQUENCE;
	ClientRequestTransport last_request_transport = ClientRequestTransport::NONE;
	string last_request_signature;
	string last_action_response;
	std::shared_ptr<arrow::Schema> last_query_schema;
	vector<std::shared_ptr<arrow::RecordBatch>> last_query_batches;
	// TODO(hjiang): Bound the in-memory query replay cache and explicitly reject replay when a result exceeds the
	// limit; consider spilling oversized replay results to object storage.
	// TODO: Persist the finished transaction watermark and outcome with authoritative data across server restarts.
};

} // namespace duckdb
