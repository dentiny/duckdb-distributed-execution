#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "server/driver/query_history.hpp"
#include "utils/mutex.hpp"
#include "worker.pb.h"

#include <memory>
#include <string_view>

namespace arrow {
namespace flight {
class FlightStreamReader;
} // namespace flight
} // namespace arrow

namespace duckdb {

class Catalog;
class Connection;
class DBConfig;
class DistributedExecutor;
class LogicalGet;
class LogicalOperator;
class TableCatalogEntry;

// An input of a fragment that tasks read from an exchange instead of scanning its table.
struct WorkerExchange {
	// Each scans part of the table and adds the task that reads each row as the last column.
	vector<string> producer_plans;
	idx_t bucket_count = 0;
};

// A part of a client's plan that workers run, serialized with PlanSerializer.
struct WorkerFragment {
	// The whole fragment, run on the driver if workers cannot run the tasks.
	string plan;
	// One plan per worker task, each reading a disjoint row group or key range of the split tables.
	vector<string> task_plans;
	// Run before the tasks, whose exchange reads scan them by index.
	vector<WorkerExchange> exchanges;
	vector<LogicalType> types;
	// Recorded in the query execution stats.
	string description;
	MergeStrategy merge_strategy = MergeStrategy::CONCATENATE;
};

// Runs fragments of a client's queries through the distributed executor, which uses its own connection because the
// client's connection is busy running the query containing the fragment.
class WorkerFragmentState : public ClientContextState {
public:
	static constexpr std::string_view NAME = "worker_fragments";

	WorkerFragmentState(DistributedExecutor &executor_p, Connection &connection_p);

	// Fills the fragment's exchanges, then starts its tasks and stores each task's result stream in streams. Adds every
	// exchange it creates to exchanges, for ReleaseExchanges. Returns false if a producer fails or a task cannot start.
	bool StartTasks(const WorkerFragment &fragment, vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                vector<distributed::ExchangeSource> &exchanges);
	void ReleaseExchanges(const vector<distributed::ExchangeSource> &exchanges);
	// Runs the whole fragment on the driver and returns its result as chunks of arbitrary size.
	vector<unique_ptr<DataChunk>> ExecuteLocally(const WorkerFragment &fragment);
	void RecordExecution(QueryExecutionInfo info);
	// Returns the fragments executed since the last call.
	vector<QueryExecutionInfo> TakeExecutions();
	idx_t NumWorkers();
	// Whether the table belongs to the catalog served by this executor.
	bool IsSameCatalog(const TableCatalogEntry &table) const;

private:
	DistributedExecutor &executor;
	Connection &connection;
	Catalog &catalog;
	// Fragments of one query can finish concurrently on scheduler threads.
	concurrency::mutex executions_mutex;
	vector<QueryExecutionInfo> executions DUCKDB_GUARDED_BY(executions_mutex);
};

// Serializes plan once per task, restricting partitioned, one of its scans, to a contiguous run of whole row
// groups per task; the last task also reads rows appended later. Creates at most max_tasks tasks, and a single
// unrestricted one if that is all there is.
vector<string> SerializeTaskPlans(ClientContext &context, LogicalOperator &plan, LogicalGet &partitioned,
                                  idx_t max_tasks);

// "auto" splits a Join when the cost model favors it, "always" whenever it has two tasks, "never" not at all.
constexpr std::string_view JOIN_PARTITIONING_SETTING = "duckherder_join_partitioning";

// Registers JOIN_PARTITIONING_SETTING.
void RegisterJoinPartitioningSetting(DBConfig &config);

// Registers the optimizer extension that replaces eligible scans, Joins and aggregates with worker fragments.
void RegisterWorkerFragments(DBConfig &config);

} // namespace duckdb
