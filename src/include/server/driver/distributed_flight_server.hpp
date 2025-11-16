#pragma once

#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/driver/query_plan_analyzer.hpp"

#include <arrow/flight/api.h>
#include <arrow/record_batch.h>
#include <memory>
#include <chrono>
#include <mutex>

namespace duckdb {

// Enum for query execution modes
enum class QueryExecutionMode {
	LOCAL,                    // Executed locally without distribution
	DISTRIBUTED_CONCATENATE,  // Distributed with simple concatenation
	DISTRIBUTED_AGGREGATE,    // Distributed with aggregate merge
	DISTRIBUTED_GROUP_BY,     // Distributed with group by merge
	DISTRIBUTED_DISTINCT      // Distributed with distinct merge
};

// Structure to store query execution information
struct QueryExecutionInfo {
	string sql;                           // The SQL query
	QueryExecutionMode execution_mode;    // How the query was executed
	std::chrono::milliseconds query_duration;  // Total query duration
	std::chrono::milliseconds worker_execution_time;  // Time spent by workers
	std::chrono::system_clock::time_point execution_start_time;  // When query started
	idx_t num_workers_used = 0;          // Number of workers used
	
	QueryExecutionInfo() 
		: execution_mode(QueryExecutionMode::LOCAL),
		  query_duration(0),
		  worker_execution_time(0),
		  execution_start_time(std::chrono::system_clock::now()) {}
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

	// Get server location.
	string GetLocation() const;

	// Register an external worker node.
	void RegisterWorker(const string &worker_id, const string &location);

	// Get the number of registered workers.
	idx_t GetWorkerCount() const;

	// Flight RPC methods.
	arrow::Status DoAction(const arrow::flight::ServerCallContext &context, const arrow::flight::Action &action,
	                       std::unique_ptr<arrow::flight::ResultStream> *result) override;

	arrow::Status DoGet(const arrow::flight::ServerCallContext &context, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

	arrow::Status DoPut(const arrow::flight::ServerCallContext &context,
	                    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
	                    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

	DatabaseInstance &GetDatabaseInstance();

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
	arrow::Status HandleExecuteSQL(const distributed::ExecuteSQLRequest &req, distributed::DistributedResponse &resp);

	// Handle CREATE TABLE request.
	// Return error status if the table already exists.
	arrow::Status HandleCreateTable(const distributed::CreateTableRequest &req, distributed::DistributedResponse &resp);

	// Handle DROP TABLE request.
	// Return OK status if the table doesn't exist.
	arrow::Status HandleDropTable(const distributed::DropTableRequest &req, distributed::DistributedResponse &resp);

	// Handle CREATE INDEX request.
	// Return error status if the index already exists.
	arrow::Status HandleCreateIndex(const distributed::CreateIndexRequest &req, distributed::DistributedResponse &resp);

	// Handle DROP INDEX request.
	// Return OK status if the index doesn't exist.
	arrow::Status HandleDropIndex(const distributed::DropIndexRequest &req, distributed::DistributedResponse &resp);

	// Handle ALTER TABLE request.
	// Return error status if the table doesn't exist or if the alteration fails.
	arrow::Status HandleAlterTable(const distributed::AlterTableRequest &req, distributed::DistributedResponse &resp);

	// Handle LOAD EXTENSION request.
	// Return error status if the extension fails to load.
	arrow::Status HandleLoadExtension(const distributed::LoadExtensionRequest &req,
	                                  distributed::DistributedResponse &resp);

	arrow::Status HandleTableExists(const distributed::TableExistsRequest &req, distributed::DistributedResponse &resp);
	arrow::Status HandleScanTable(const distributed::ScanTableRequest &req,
	                              std::unique_ptr<arrow::flight::FlightDataStream> &stream);
	arrow::Status HandleInsertData(const std::string &table_name, std::shared_ptr<arrow::RecordBatch> batch,
	                               distributed::DistributedResponse &resp);

	// Convert DuckDB result to Arrow RecordBatch.
	arrow::Status QueryResultToArrow(QueryResult &result, std::shared_ptr<arrow::RecordBatchReader> &reader,
	                                 idx_t *row_count = nullptr);

	// Record query execution information
	void RecordQueryExecution(const QueryExecutionInfo &info);

	// Get all recorded query executions (for debugging/inspection)
	vector<QueryExecutionInfo> GetQueryExecutions() const;

private:
	string host;
	int port;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> conn;
	unique_ptr<WorkerManager> worker_manager;
	unique_ptr<DistributedExecutor> distributed_executor;

	// Query execution tracking
	mutable std::mutex query_history_mutex;
	vector<QueryExecutionInfo> query_history;
};

} // namespace duckdb
