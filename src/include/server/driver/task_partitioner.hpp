#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "server/driver/query_plan_analyzer.hpp"

namespace duckdb {

// Forward declarations.
struct DistributedPipelineTask;
class SelectStatement;

// Creates distributed tasks from logical plans.
// Assigns each worker a contiguous run of whole row groups when storage bounds are available.
class TaskPartitioner {
public:
	TaskPartitioner(Connection &conn, QueryPlanAnalyzer &analyzer);

	// Whether `statement`, a two-table Join aggregate over the scans in `logical_plan`, can be split into row-group
	// tasks whose partial aggregates the driver merges.
	bool CanPartitionJoin(LogicalOperator &logical_plan, const SelectStatement &statement);

	// Extract distributed pipeline tasks from logical plan.
	// Creates tasks aligned with native DuckDB row groups when possible.
	vector<DistributedPipelineTask> ExtractPipelineTasks(LogicalOperator &logical_plan, const string &base_sql,
	                                                     idx_t num_workers);

private:
	// Create a single non-distributed task.
	vector<DistributedPipelineTask> CreateSingleTask(const string &base_sql);

	Connection &conn;
	QueryPlanAnalyzer &analyzer;
};

} // namespace duckdb
