#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

// Forward declarations.
struct DistributedPipelineTask;
class PartitionSQLGenerator;
class QueryPlanAnalyzer;

// Creates distributed tasks from logical plans.
// Uses intelligent partitioning strategies (row group, range, or modulo).
class TaskPartitioner {
public:
	TaskPartitioner(Connection &conn, QueryPlanAnalyzer &analyzer, PartitionSQLGenerator &sql_gen);

	// Extract distributed pipeline tasks from logical plan.
	// Creates tasks with intelligent partitioning based on plan analysis.
	vector<DistributedPipelineTask> ExtractPipelineTasks(LogicalOperator &logical_plan, const string &base_sql,
	                                                     idx_t num_workers);

private:
	// Determine if distribution is needed based on table size.
	// Returns false for small tables (< 1 row group) that should execute on a single node.
	bool ShouldDistribute(idx_t estimated_cardinality) const;

	// Create a single non-distributed task.
	vector<DistributedPipelineTask> CreateSingleTask(const string &base_sql);

	Connection &conn;
	QueryPlanAnalyzer &analyzer;
	PartitionSQLGenerator &sql_generator;
};

} // namespace duckdb
