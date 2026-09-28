#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "server/driver/client_registration.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/distributed_flight_server_test_state.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/worker_manager.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <chrono>
#include <memory>
#include <shared_mutex>

namespace duckdb {

// Enum for query execution modes based on partitioning strategy
enum class QueryExecutionMode {
	LOCAL,              // Local execution on driver (no distribution)
	DELEGATED,          // No partition - delegated to single worker node
	NATURAL_PARTITION,  // Distributed with natural parallelism (based on DuckDB's estimation)
	ROW_GROUP_PARTITION // Distributed with row-group-aligned partitioning
};

// Structure to store query execution information.
struct QueryExecutionInfo {
	string sql;                                                 // The SQL query
	QueryExecutionMode execution_mode;                          // Partitioning strategy used
	QueryPlanAnalyzer::MergeStrategy merge_strategy;            // How results were merged
	std::chrono::milliseconds query_duration;                   // Total query duration
	std::chrono::system_clock::time_point execution_start_time; // When query started (wall-clock time)
	idx_t num_workers_used = 0;                                 // Number of workers used
	idx_t num_tasks_generated = 0;                              // Number of tasks created

	QueryExecutionInfo()
	    : execution_mode(QueryExecutionMode::LOCAL), merge_strategy(QueryPlanAnalyzer::MergeStrategy::CONCATENATE),
	      query_duration(0), execution_start_time(std::chrono::system_clock::now()) {
	}
};

// Arrow Flight-based RPC server for distributed execution.
class DistributedFlightServer : public arrow::flight::FlightServerBase {
public:
	explicit DistributedFlightServer(string host_p = "0.0.0.0", int port_p = 8815);
	~DistributedFlightServer() override = default;

	// Start the server.
	arrow::Status Start();

	// Start server with worker nodes.
	// Only used to create local worker nodes for testing and dev.
	arrow::Status StartWithWorkers(idx_t num_workers);

	// Start a number of local worker nodes in background threads.
	// Only used for local testing and dev.
	void StartLocalWorkers(idx_t num_workers);

	// Stop the server.
	void Shutdown();

	// Reset all server states.
	void Reset();

	// Get server location.
	string GetLocation() const;

	// Register an external worker node.
	void RegisterWorker(const string &worker_id, const string &location);

	// Register or replace the driver node.
	// Unlike workers, only one driver node can be registered at a time.
	void RegisterOrReplaceDriver(const string &driver_id, const string &location);

	// Get the number of registered workers.
	idx_t GetWorkerCount() const;

	// Record query execution information.
	void RecordQueryExecution(QueryExecutionInfo info);

	// Get all recorded query executions.
	vector<QueryExecutionInfo> GetQueryExecutions() const;

	// Flight RPC methods.
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

	arrow::Status DoPut(const arrow::flight::ServerCallContext &context,
	                    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
	                    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

	DatabaseInstance &GetDatabaseInstance();

	// Test hooks.
	DistributedFlightServerTestState &GetTestStateForTesting();

private:
	// Implementation methods for Flight RPC handlers, without exception handling.
	arrow::Status DoActionImpl(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                           std::unique_ptr<arrow::flight::ResultStream> *result);

	arrow::Status DoGetImpl(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                        std::unique_ptr<arrow::flight::FlightDataStream> *stream);

	arrow::Status DoPutImpl(const arrow::flight::ServerCallContext &context,
	                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
	                        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer);

	// Process different request types using protobuf messages directly.
	arrow::Status HandleRegisterClient(const distributed::RegisterClientRequest &req,
	                                   distributed::DistributedResponse &resp);
	arrow::Status HandleUnregisterClient(const string &client_id, distributed::DistributedResponse &resp);
	arrow::Status HandleTransaction(const distributed::DistributedRequest &req, ClientRegistration &registration,
	                                distributed::DistributedResponse &resp);
	arrow::Status HandleExecuteStatement(const distributed::ExecuteStatementRequest &req,
	                                     ClientRegistration &registration, distributed::DistributedResponse &resp);

	// Handle LOAD EXTENSION request.
	// Return error status if the extension fails to load.
	arrow::Status HandleLoadExtension(const distributed::LoadExtensionRequest &req, ClientRegistration &registration,
	                                  distributed::DistributedResponse &resp);

	// Handle GET QUERY EXECUTION STATS request.
	// Return query execution statistics from the server.
	arrow::Status HandleGetQueryExecutionStats(const distributed::GetQueryExecutionStatsRequest &req,
	                                           distributed::DistributedResponse &resp);

	arrow::Status HandleTableExists(const distributed::TableExistsRequest &req, ClientRegistration &registration,
	                                distributed::DistributedResponse &resp);
	arrow::Status HandleScanTable(const distributed::ScanTableRequest &req, ClientRegistration &registration,
	                              std::shared_ptr<arrow::Schema> &schema,
	                              vector<std::shared_ptr<arrow::RecordBatch>> &batches);
	arrow::Status HandleInsertData(const std::string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                               ClientRegistration &registration, distributed::DistributedResponse &resp);

	// Convert DuckDB result to Arrow RecordBatch.
	arrow::Status QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::Schema> &schema,
	                                 vector<std::shared_ptr<arrow::RecordBatch>> &batches, idx_t *row_count = nullptr);

	// Initialize DuckDB instance, connection, and components.
	void Initialize();

	// Look up a registration while the caller holds clients_mutex.
	bool LookupClient(const string &client_id, shared_ptr<ClientRegistration> &registration);
	// Renew a client's lease after an authorized request.
	void TouchClient(const shared_ptr<ClientRegistration> &registration);
	// Remove expired registrations while the caller holds clients_mutex exclusively.
	void PruneExpiredClients();
	// Validate registration against the minimum role while holding clients_mutex, renewing the lease on success.
	bool AuthorizeClient(const string &client_id, distributed::ClientRole required_role,
	                     shared_ptr<ClientRegistration> &registration, distributed::DistributedResponse &resp);
	// Validate a transaction-scoped request and indicate whether its latest result can be replayed.
	arrow::Status CheckRequestReplay(const distributed::DistributedRequest &request,
	                                 const ClientRegistration &registration, ClientRequestTransport transport,
	                                 const string &signature, bool &replay);
	// Replace the bounded per-client replay entry after an operation has completed.
	void CacheActionResponse(const distributed::DistributedRequest &request, ClientRegistration &registration,
	                         ClientRequestTransport transport, const string &signature,
	                         const distributed::DistributedResponse &response);
	void ClearRequestReplay(ClientRegistration &registration);
	string host;
	int port;
	unique_ptr<DuckDB> db;
	unique_ptr<WorkerManager> worker_manager;

	// Client admission: at most one writable attachment, with any number of readers.
	mutable std::shared_mutex clients_mutex;
	unordered_map<string, shared_ptr<ClientRegistration>> clients;
	string writable_client_id;
	DistributedFlightServerTestState test_state;

	// Query execution tracking.
	mutable mutex query_history_mutex;
	vector<QueryExecutionInfo> query_history;
};

} // namespace duckdb
