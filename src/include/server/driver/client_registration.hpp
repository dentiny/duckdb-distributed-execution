#pragma once

#include "client.pb.h"
#include "storage.pb.h"
#include "transaction.pb.h"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/shared_ptr.hpp"
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
	// db is either the Duckling instance or the instance dedicated to storage_config.
	ClientRegistration(shared_ptr<DuckDB> db, WorkerManager &worker_manager, distributed::ClientRole role_p,
	                   const distributed::StorageConfig &storage_config);
	~ClientRegistration();

	distributed::ClientRole role;
	// GetStorageKey of the database this client is attached to.
	const string database_key;
	// Declared before the connection so the instance outlives every session opened on it.
	shared_ptr<DuckDB> database;
	// Last authorized request time in steady-clock milliseconds, updated concurrently by RPC handlers.
	atomic<int64_t> last_seen;
	// DuckDB connections are session-scoped and must not execute concurrent requests.
	concurrency::mutex connection_mutex;
	// Dedicated DuckDB session for this registered client.
	unique_ptr<Connection> connection DUCKDB_GUARDED_BY(connection_mutex);
	// Distributed execution components bound to this client's DuckDB session; null when queries must run locally.
	unique_ptr<DistributedExecutor> distributed_executor DUCKDB_GUARDED_BY(connection_mutex);
	// One connection has at most one active transaction. The finished watermark and its outcome make retries
	// idempotent with constant memory; older outcomes no longer need to be replayed after a newer transaction
	// starts.
	uint64_t active_transaction_id DUCKDB_GUARDED_BY(connection_mutex) = INVALID_TRANSACTION_ID;
	uint64_t finished_transaction_id DUCKDB_GUARDED_BY(connection_mutex) = INVALID_TRANSACTION_ID;
	distributed::TransactionStatus
	    finished_transaction_status DUCKDB_GUARDED_BY(connection_mutex) = distributed::TRANSACTION_STATUS_UNKNOWN;
	// Only the latest request in an active transaction is retained. A retry with the same sequence replays this
	// result, while a newer sequence replaces it, keeping replay memory bounded apart from the latest query result.
	uint64_t last_request_sequence DUCKDB_GUARDED_BY(connection_mutex) = INVALID_REQUEST_SEQUENCE;
	ClientRequestTransport last_request_transport DUCKDB_GUARDED_BY(connection_mutex) = ClientRequestTransport::NONE;
	string last_request_signature DUCKDB_GUARDED_BY(connection_mutex);
	string last_action_response DUCKDB_GUARDED_BY(connection_mutex);
	std::shared_ptr<arrow::Schema> last_query_schema DUCKDB_GUARDED_BY(connection_mutex);
	vector<std::shared_ptr<arrow::RecordBatch>> last_query_batches DUCKDB_GUARDED_BY(connection_mutex);
	// TODO(hjiang): Bound the in-memory query replay cache and explicitly reject replay when a result exceeds the
	// limit; consider spilling oversized replay results to object storage.
	// TODO: Persist the finished transaction watermark and outcome with authoritative data across server restarts.
};

} // namespace duckdb
