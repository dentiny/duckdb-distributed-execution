#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "server/driver/query_history.hpp"

namespace duckdb {

class Catalog;
class Connection;
class DistributedExecutor;
class LogicalOperator;
class TableCatalogEntry;

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
	// Whether workers can run `sql`, rendered from the Join aggregate `plan`, as partitioned tasks.
	bool CanPartitionJoin(LogicalOperator &plan, const string &sql);
	// Whether the table belongs to the catalog served by this executor.
	bool IsSameCatalog(const TableCatalogEntry &table) const;

private:
	DistributedExecutor &executor;
	Connection &connection;
	Catalog &catalog;
	vector<QueryExecutionInfo> executions;
};

// Replaces eligible single-table fragments or the aggregate over a two-table Join with a worker fragment.
OptimizerExtension GetWorkerFragmentExtension();

} // namespace duckdb
