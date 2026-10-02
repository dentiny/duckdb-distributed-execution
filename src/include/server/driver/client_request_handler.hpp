#pragma once

#include "distributed.pb.h"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "server/driver/client_registration.hpp"

#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <memory>

namespace duckdb {

class DatabaseInstance;
class QueryHistory;

// Runs authorized client requests on the client's DuckDB session. A retried request replays the cached result of
// its first execution instead of running again.
class ClientRequestHandler {
public:
	// `db_instance` is only used for logging.
	ClientRequestHandler(DatabaseInstance &db_instance, QueryHistory &query_history);

	// Run an ExecuteStatement, TableExists, LoadExtension, or GetQueryExecutionStats action.
	arrow::Status HandleAction(const distributed::DistributedRequest &request, ClientRegistration &registration,
	                           distributed::DistributedResponse &response)
	    DUCKDB_REQUIRES(registration.connection_mutex);

	// Run a scan, leaving its result in the registration's query replay cache.
	arrow::Status HandleScan(const distributed::DistributedRequest &request, ClientRegistration &registration)
	    DUCKDB_REQUIRES(registration.connection_mutex);

	// Insert `batches` into `table_name`, stopping at the first failed batch. `signature` identifies the payload.
	arrow::Status HandleInsert(const distributed::DistributedRequest &request_identity, const string &table_name,
	                           const vector<std::shared_ptr<arrow::RecordBatch>> &batches, const string &signature,
	                           ClientRegistration &registration, distributed::DistributedResponse &response)
	    DUCKDB_REQUIRES(registration.connection_mutex);

private:
	arrow::Status ExecuteStatement(const distributed::ExecuteStatementRequest &req, ClientRegistration &registration,
	                               distributed::DistributedResponse &resp)
	    DUCKDB_REQUIRES(registration.connection_mutex);
	// Install and load an extension, reporting failures in `resp`.
	arrow::Status LoadExtension(const distributed::LoadExtensionRequest &req, ClientRegistration &registration,
	                            distributed::DistributedResponse &resp) DUCKDB_REQUIRES(registration.connection_mutex);
	arrow::Status TableExists(const distributed::TableExistsRequest &req, ClientRegistration &registration,
	                          distributed::DistributedResponse &resp) DUCKDB_REQUIRES(registration.connection_mutex);
	arrow::Status ScanTable(const distributed::ScanTableRequest &req, ClientRegistration &registration,
	                        std::shared_ptr<arrow::Schema> &schema,
	                        vector<std::shared_ptr<arrow::RecordBatch>> &batches)
	    DUCKDB_REQUIRES(registration.connection_mutex);
	arrow::Status InsertData(const string &table_name, const std::shared_ptr<arrow::RecordBatch> &batch,
	                         ClientRegistration &registration, distributed::DistributedResponse &resp)
	    DUCKDB_REQUIRES(registration.connection_mutex);

	DatabaseInstance &db_instance;
	QueryHistory &query_history;
};

} // namespace duckdb
