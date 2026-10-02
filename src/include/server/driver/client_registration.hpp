#pragma once

#include "client.pb.h"
#include "distributed.pb.h"
#include "storage_config.pb.h"
#include "transaction.pb.h"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "transaction_constants.hpp"
#include "utils/mutex.hpp"

#include <arrow/status.h>
#include <memory>

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace duckdb {

class Connection;
class DistributedExecutor;
class DuckDB;
class ObjectStorageDatabase;
class WorkerFragmentState;
class WorkerManager;

enum class ClientRequestTransport : uint8_t { NONE, ACTION, DO_GET, DO_PUT };

// Owns the Control Node resources and bounded transaction replay state for one registered client.
struct ClientRegistration {
	// Queries run locally when `executor_connection_p` is null.
	ClientRegistration(ObjectStorageDatabase &db, unique_ptr<Connection> connection_p,
	                   unique_ptr<Connection> executor_connection_p, WorkerManager &worker_manager,
	                   distributed::ClientRole role_p, const distributed::StorageConfig &storage_config);
	~ClientRegistration();

	// Validate a transaction-scoped request and indicate whether its latest result can be replayed.
	arrow::Status CheckRequestReplay(const distributed::DistributedRequest &request, ClientRequestTransport transport,
	                                 const string &signature, bool &replay) const DUCKDB_REQUIRES(connection_mutex);
	// Replace the bounded replay entry after an action or insertion has completed.
	void CacheActionResponse(const distributed::DistributedRequest &request, ClientRequestTransport transport,
	                         const string &signature, const distributed::DistributedResponse &response)
	    DUCKDB_REQUIRES(connection_mutex);
	// Replace the bounded replay entry after a scan has completed.
	void CacheQueryResult(const distributed::DistributedRequest &request, const string &signature,
	                      std::shared_ptr<arrow::Schema> schema, vector<std::shared_ptr<arrow::RecordBatch>> batches)
	    DUCKDB_REQUIRES(connection_mutex);
	void ClearRequestReplay() DUCKDB_REQUIRES(connection_mutex);

	distributed::ClientRole role;
	// Storage identity, excluding credentials, for the database this client is attached to.
	const string database_key;
	// Declared before the connection so the instance outlives every session opened on it.
	shared_ptr<DuckDB> database;
	// Last authorized request time in steady-clock milliseconds, updated concurrently by RPC handlers.
	atomic<int64_t> last_seen;
	// DuckDB connections are session-scoped and must not execute concurrent requests.
	concurrency::mutex connection_mutex;
	// Dedicated DuckDB session for this registered client.
	unique_ptr<Connection> connection DUCKDB_GUARDED_BY(connection_mutex);
	// Distributed execution components for fragments of this client's queries; null when queries must run locally.
	unique_ptr<Connection> executor_connection DUCKDB_GUARDED_BY(connection_mutex);
	unique_ptr<DistributedExecutor> distributed_executor DUCKDB_GUARDED_BY(connection_mutex);
	shared_ptr<WorkerFragmentState> worker_fragments DUCKDB_GUARDED_BY(connection_mutex);
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

private:
	// Start a replay entry for a completed request, finishing its transaction when it ran in autocommit mode.
	void RecordCompletedRequest(const distributed::DistributedRequest &request, ClientRequestTransport transport,
	                            const string &signature) DUCKDB_REQUIRES(connection_mutex);
};

} // namespace duckdb
