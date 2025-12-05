#include "server/driver/task_partitioner.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

TaskPartitioner::TaskPartitioner(Connection &conn_p, QueryPlanAnalyzer &analyzer_p, PartitionSQLGenerator &sql_gen_p)
    : conn(conn_p), analyzer(analyzer_p), sql_generator(sql_gen_p) {
}

bool TaskPartitioner::ShouldDistribute(idx_t estimated_cardinality) const {
	// Don't distribute small tables, which contains less than one row group.
	// These execute more efficiently on a single node without distribution overhead.
	if (estimated_cardinality > 0 && estimated_cardinality < DEFAULT_ROW_GROUP_SIZE) {
		return false;
	}
	return true;
}

vector<DistributedPipelineTask> TaskPartitioner::CreateSingleTask(const string &base_sql) {
	vector<DistributedPipelineTask> tasks;
	DistributedPipelineTask task;
	task.task_id = 0;
	task.total_tasks = 1;
	task.task_sql = base_sql;
	task.row_group_start = 0;
	task.row_group_end = 0;
	tasks.emplace_back(std::move(task));
	return tasks;
}

vector<DistributedPipelineTask> TaskPartitioner::ExtractPipelineTasks(LogicalOperator &logical_plan,
                                                                      const string &base_sql, idx_t num_workers) {
	vector<DistributedPipelineTask> tasks;

	// Extract row group information for DuckDB-aligned partitioning
	auto row_group_info = analyzer.ExtractRowGroupInfo(logical_plan);

	// First, get partition info (i.e., cardinality, estimated parallelism)
	auto partition_info = analyzer.ExtractPartitionInfo(logical_plan, num_workers);

	// Determine how many tasks to create
	// For now, we use max(estimated_parallelism, num_workers)
	// This allows us to over-subscribe workers if DuckDB wants more parallelism
	idx_t num_tasks = partition_info.estimated_parallelism;
	if (num_tasks == 0 || num_tasks < num_workers) {
		// DuckDB doesn't think this needs parallelism, but we have workers available
		// Use num_workers anyway for distributed execution
		// TODO(hjiang): Revisit later, if duckdb doesn't think we need parallelism, we simply execute inline.
		num_tasks = num_workers;
	} else if (num_tasks > num_workers * 4) {
		// Cap at 4x workers to avoid too many small tasks
		num_tasks = num_workers * 4;
	}

	// Case-1: row group-based partitioning is available.
	// But only use it if we have enough row groups to satisfy the desired task count
	// Otherwise fall through to range-based partitioning
	if (row_group_info.valid && row_group_info.total_row_groups > 0 && 
	    row_group_info.total_row_groups >= num_tasks) {
		// Assign row groups to tasks
		// If we have more tasks than row groups, some tasks will be empty
		// If we have fewer tasks than row groups, distribute row groups across tasks
		const idx_t row_groups_per_task = (row_group_info.total_row_groups + num_tasks - 1) / num_tasks;

		for (idx_t idx = 0; idx < num_tasks; ++idx) {
			DistributedPipelineTask task;
			task.task_id = idx;
			task.total_tasks = num_tasks;

			// Calculate which row groups this task processes
			const idx_t rg_start = idx * row_groups_per_task;
			const idx_t rg_end = std::min((idx + 1) * row_groups_per_task, row_group_info.total_row_groups);

			// Skip empty tasks
			if (rg_start >= row_group_info.total_row_groups) {
				continue;
			}

			// Calculate row boundaries based on row group boundaries
			const idx_t row_start = rg_start * row_group_info.rows_per_row_group;
			const idx_t row_end =
			    std::min(rg_end * row_group_info.rows_per_row_group, partition_info.estimated_cardinality);

			// Create SQL with row group-aligned rowid filter
			string where_condition =
			    StringUtil::Format("rowid BETWEEN %llu AND %llu", static_cast<long long unsigned>(row_start),
			                       static_cast<long long unsigned>(row_end - 1));
			task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);
			task.row_group_start = rg_start;
			task.row_group_end = rg_end - 1;
			tasks.emplace_back(std::move(task));
		}

		return tasks;
	}

	// Case-2: perform range-based partition.
	if (partition_info.supports_intelligent_partitioning && partition_info.estimated_cardinality > 0) {
		// For small tables (less than one row group), don't distribute - use single task
		if (!ShouldDistribute(partition_info.estimated_cardinality)) {
			return CreateSingleTask(base_sql);
		}

		idx_t rows_per_task = (partition_info.estimated_cardinality + num_tasks - 1) / num_tasks;

		for (idx_t idx = 0; idx < num_tasks; ++idx) {
			DistributedPipelineTask task;
			task.task_id = idx;
			task.total_tasks = num_tasks;

			// Calculate rowid range for this task
			const idx_t row_start = idx * rows_per_task;
			idx_t row_end = std::min((idx + 1) * rows_per_task - 1, partition_info.estimated_cardinality);

			// For last task, extend to include any remainder
			if (idx == num_tasks - 1) {
				row_end = partition_info.estimated_cardinality;
			}

			// Create SQL with rowid filter
			string where_condition =
			    StringUtil::Format("rowid BETWEEN %llu AND %llu", static_cast<long long unsigned>(row_start),
			                       static_cast<long long unsigned>(row_end));
			task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);

			task.row_group_start = row_start / DEFAULT_ROW_GROUP_SIZE;
			task.row_group_end = row_end / DEFAULT_ROW_GROUP_SIZE;

			tasks.emplace_back(std::move(task));
		}

		return tasks;
	}

	// Case-3: Fallback to modulo-based partitioning.
	// For small tables (less than one row group), don't distribute.
	if (!ShouldDistribute(partition_info.estimated_cardinality)) {
		return CreateSingleTask(base_sql);
	}

	for (idx_t idx = 0; idx < num_tasks; ++idx) {
		DistributedPipelineTask task;
		task.task_id = idx;
		task.total_tasks = num_tasks;

		// Create SQL with modulo filter
		string where_condition = StringUtil::Format(
		    "(rowid %% %llu) = %llu", static_cast<long long unsigned>(num_tasks), static_cast<long long unsigned>(idx));
		task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);

		// Row group information unknown for modulo partitioning.
		task.row_group_start = 0;
		task.row_group_end = 0;

		tasks.emplace_back(std::move(task));
	}

	return tasks;
}

} // namespace duckdb
