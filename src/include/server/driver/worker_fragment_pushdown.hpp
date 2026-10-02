#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "server/driver/query_history.hpp"

namespace duckdb {

class Connection;
class DistributedExecutor;

// Runs fragments of a client's queries through the distributed executor, which uses its own connection because the
// client's connection is busy running the query containing the fragment.
class WorkerFragmentState : public ClientContextState {
public:
	static constexpr const char *NAME = "worker_fragments";

	WorkerFragmentState(DistributedExecutor &executor_p, Connection &connection_p);

	// Returns the result of `sql`, whose columns have `types`, as chunks of arbitrary size.
	vector<unique_ptr<DataChunk>> Execute(ClientContext &context, const string &sql, const vector<LogicalType> &types);
	// Returns the fragments executed since the last call.
	vector<QueryExecutionInfo> TakeExecutions();

private:
	DistributedExecutor &executor;
	Connection &connection;
	vector<QueryExecutionInfo> executions;
};

// Replaces the single-table part of a query with a fragment the distributed executor runs on workers: an aggregate
// over the table if it can be partially computed per partition, otherwise the table scan. Plans reading several
// tables, such as joins, run on the driver.
OptimizerExtension GetWorkerFragmentExtension();

} // namespace duckdb
