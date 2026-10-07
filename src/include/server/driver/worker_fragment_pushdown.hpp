#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "server/driver/query_history.hpp"

#include <optional>

namespace duckdb {

class Connection;
class DistributedExecutor;
class LogicalOperator;
class PreparedStatement;
class SelectStatement;

enum class DistributedFragmentKind;

// Runs fragments of a client's queries through the distributed executor, which uses its own connection because the
// client's connection is busy running the query containing the fragment.
class WorkerFragmentState : public ClientContextState {
public:
	static constexpr const char *NAME = "worker_fragments";

	WorkerFragmentState(DistributedExecutor &executor_p, Connection &connection_p);

	// Returns the result of `sql`, whose columns have `types`, as chunks of arbitrary size.
	vector<unique_ptr<DataChunk>> Execute(ClientContext &context, const string &sql, const vector<LogicalType> &types,
	                                      DistributedFragmentKind kind);
	// Returns the fragments executed since the last call.
	vector<QueryExecutionInfo> TakeExecutions();
	// The optimizer runs during Prepare and needs the original SQL to build Join task queries.
	// Expose it only for this client query, not for later prepares on the same connection.
	unique_ptr<PreparedStatement> PrepareClientQuery(Connection &client_connection, const string &sql);
	const string *PlanningQuery() const;
	bool CanPartitionJoin(LogicalOperator &plan, const SelectStatement &statement);

private:
	DistributedExecutor &executor;
	Connection &connection;
	vector<QueryExecutionInfo> executions;
	std::optional<string> planning_query;
};

// Replaces eligible single-table fragments or a complete two-table Join aggregate with a worker fragment.
OptimizerExtension GetWorkerFragmentExtension();

} // namespace duckdb
