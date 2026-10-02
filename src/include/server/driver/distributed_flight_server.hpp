#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "server/driver/client_registry.hpp"
#include "server/driver/client_request_handler.hpp"
#include "server/driver/distributed_flight_server_test_state.hpp"
#include "server/driver/query_history.hpp"
#include "server/driver/transaction_request_handler.hpp"
#include "server/driver/worker_manager.hpp"

#include <arrow/flight/api.h>
#include <memory>

namespace duckdb {

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
	arrow::Status StartLocalWorkers(idx_t num_workers);

	// Stop the server.
	void Shutdown();

	// Reset all server states.
	void Reset();

	// Get server location.
	string GetLocation() const;

	// Register an external worker node.
	arrow::Status RegisterWorker(const string &worker_id, const string &location);

	// Register or replace the driver node.
	// Unlike workers, only one driver node can be registered at a time.
	arrow::Status RegisterOrReplaceDriver(const string &driver_id, const string &location);

	// Get the number of registered workers.
	idx_t GetWorkerCount() const;

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

	// Authorize a client request against `required_role` and run it on the client's session.
	arrow::Status HandleClientAction(const distributed::DistributedRequest &request,
	                                 distributed::ClientRole required_role, distributed::DistributedResponse &response)
	    DUCKDB_REQUIRES_SHARED(clients.mutex);

	// Initialize DuckDB instance, connection, and components.
	void Initialize();

	string host;
	int port;
	// Server-owned utility instance used for logging and worker management.
	shared_ptr<DuckDB> db;
	unique_ptr<WorkerManager> worker_manager;

	DistributedFlightServerTestState test_state;
	ClientRegistry clients {test_state};
	QueryHistory query_history;
	TransactionRequestHandler transaction_handler {test_state};
	// Recreated with `db`, which it logs to.
	unique_ptr<ClientRequestHandler> request_handler;
};

} // namespace duckdb
