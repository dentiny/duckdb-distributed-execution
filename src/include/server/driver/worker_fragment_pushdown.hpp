#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "server/driver/query_history.hpp"

namespace duckdb {

class Catalog;
class Connection;
class DistributedExecutor;
class LogicalGet;
class LogicalOperator;
class TableCatalogEntry;

// A part of a client's plan that workers run, serialized with PlanSerializer.
struct WorkerFragment {
	// A Join aggregate rendered as SQL, which the executor partitions itself; the plans are then empty.
	string sql;
	// The whole fragment, run on the driver if workers cannot run the tasks.
	string plan;
	// One plan per worker task, each reading a disjoint range of row groups of the partitioned table.
	vector<string> task_plans;
	vector<LogicalType> types;
	// Recorded in the query execution stats.
	string description;
	QueryPlanAnalyzer::MergeStrategy merge_strategy = QueryPlanAnalyzer::MergeStrategy::CONCATENATE;
};

// Runs fragments of a client's queries through the distributed executor, which uses its own connection because the
// client's connection is busy running the query containing the fragment.
class WorkerFragmentState : public ClientContextState {
public:
	static constexpr const char *NAME = "worker_fragments";

	WorkerFragmentState(DistributedExecutor &executor_p, Connection &connection_p);

	// Returns the concatenated results of the fragment's tasks as chunks of arbitrary size.
	vector<unique_ptr<DataChunk>> Execute(ClientContext &context, const WorkerFragment &fragment);
	// Returns the fragments executed since the last call.
	vector<QueryExecutionInfo> TakeExecutions();
	idx_t NumWorkers();
	// Whether workers can run `sql`, rendered from the Join aggregate `plan`, as partitioned tasks.
	bool CanPartitionJoin(LogicalOperator &plan, const string &sql);
	// Whether the table belongs to the catalog served by this executor.
	bool IsSameCatalog(const TableCatalogEntry &table) const;

private:
	vector<unique_ptr<DataChunk>> ExecuteJoinSQL(ClientContext &context, const string &sql,
	                                             const vector<LogicalType> &types);

	DistributedExecutor &executor;
	Connection &connection;
	Catalog &catalog;
	vector<QueryExecutionInfo> executions;
};

// Serializes plan once per task, restricting partitioned, one of its scans, to a contiguous run of whole row
// groups per task; the last task also reads rows appended later. Creates at most max_tasks tasks, and a single
// unrestricted one if that is all there is.
vector<string> SerializeTaskPlans(ClientContext &context, LogicalOperator &plan, LogicalGet &partitioned,
                                  idx_t max_tasks);

// Replaces eligible scans and aggregates, over one table or an inner Join of two, with worker fragments.
OptimizerExtension GetWorkerFragmentExtension();

} // namespace duckdb
