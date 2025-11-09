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

vector<DistributedPipelineTask> TaskPartitioner::ExtractPipelineTasks(LogicalOperator &logical_plan,
                                                                      const string &base_sql, idx_t num_workers) {

	auto &db_instance = *conn.context->db;
	vector<DistributedPipelineTask> tasks;

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔧 [PIPELINE] Extracting tasks: base_sql='%s' workers=%llu",
	                                                 base_sql, static_cast<long long unsigned>(num_workers)));

	try {
		// STEP 5: Extract row group information for DuckDB-aligned partitioning
		auto row_group_info = analyzer.ExtractRowGroupInfo(logical_plan);

		// First, get partition info (cardinality, natural parallelism)
		auto partition_info = analyzer.ExtractPartitionInfo(logical_plan, num_workers);

		// Determine how many tasks to create
		// For now, we use max(natural_parallelism, num_workers)
		// This allows us to over-subscribe workers if DuckDB wants more parallelism
		idx_t num_tasks = partition_info.natural_parallelism;
		if (num_tasks == 0 || num_tasks < num_workers) {
			// DuckDB doesn't think this needs parallelism, but we have workers available
			// Use num_workers anyway for distributed execution
			num_tasks = num_workers;
			DUCKDB_LOG_DEBUG(
			    db_instance,
			    StringUtil::Format("🔧 [PIPELINE] Natural parallelism %llu < workers %llu, promoting to %llu tasks",
			                       static_cast<long long unsigned>(partition_info.natural_parallelism),
			                       static_cast<long long unsigned>(num_workers),
			                       static_cast<long long unsigned>(num_tasks)));
		} else if (num_tasks > num_workers * 4) {
			// Cap at 4x workers to avoid too many small tasks
			num_tasks = num_workers * 4;
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format(
			                                  "🔧 [PIPELINE] Capping tasks at %llu (4x workers) to avoid fragmentation",
			                                  static_cast<long long unsigned>(num_tasks)));
		}

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔧 [PIPELINE] Creating %llu pipeline tasks",
		                                                 static_cast<long long unsigned>(num_tasks)));

		// STEP 5: Prefer row group-based partitioning when available
		if (row_group_info.valid && row_group_info.total_row_groups > 0) {
			// ROW GROUP-BASED PARTITIONING (Step 5): Align with DuckDB's natural storage structure
			DUCKDB_LOG_DEBUG(
			    db_instance,
			    StringUtil::Format("🔧 [PIPELINE] Using row group partitioning: groups=%llu rows_per_group=%llu",
			                       static_cast<long long unsigned>(row_group_info.total_row_groups),
			                       static_cast<long long unsigned>(row_group_info.rows_per_row_group)));

			// Assign row groups to tasks
			// If we have more tasks than row groups, some tasks will be empty
			// If we have fewer tasks than row groups, distribute row groups across tasks
			idx_t row_groups_per_task = (row_group_info.total_row_groups + num_tasks - 1) / num_tasks;

			for (idx_t i = 0; i < num_tasks; i++) {
				DistributedPipelineTask task;
				task.task_id = i;
				task.total_tasks = num_tasks;

				// Calculate which row groups this task processes
				idx_t rg_start = i * row_groups_per_task;
				idx_t rg_end = std::min((i + 1) * row_groups_per_task, row_group_info.total_row_groups);

				// Skip empty tasks
				if (rg_start >= row_group_info.total_row_groups) {
					continue;
				}

				// Calculate row boundaries based on row group boundaries
				idx_t row_start = rg_start * row_group_info.rows_per_row_group;
				idx_t row_end =
				    std::min(rg_end * row_group_info.rows_per_row_group, partition_info.estimated_cardinality);

				// Create SQL with row group-aligned rowid filter
				string where_condition =
				    StringUtil::Format("rowid BETWEEN %llu AND %llu", static_cast<long long unsigned>(row_start),
				                       static_cast<long long unsigned>(row_end - 1));
				task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);

				task.row_group_start = rg_start;
				task.row_group_end = rg_end - 1;

				DUCKDB_LOG_DEBUG(db_instance,
				                 StringUtil::Format("🗂️  [STEP5] Task %llu: row groups [%llu, %llu], rows [%llu, %llu]",
				                                    static_cast<long long unsigned>(i),
				                                    static_cast<long long unsigned>(rg_start),
				                                    static_cast<long long unsigned>(rg_end - 1),
				                                    static_cast<long long unsigned>(row_start),
				                                    static_cast<long long unsigned>(row_end - 1)));

				tasks.push_back(std::move(task));
			}
		} else if (partition_info.supports_intelligent_partitioning && partition_info.estimated_cardinality > 0) {
			// Fallback to intelligent range-based partitioning
			DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format(
			                                  "🔧 [PIPELINE] Using range-based partitioning fallback cardinality=%llu",
			                                  static_cast<long long unsigned>(partition_info.estimated_cardinality)));

			idx_t rows_per_task = (partition_info.estimated_cardinality + num_tasks - 1) / num_tasks;

			for (idx_t i = 0; i < num_tasks; i++) {
				DistributedPipelineTask task;
				task.task_id = i;
				task.total_tasks = num_tasks;

				// Calculate rowid range for this task
				idx_t row_start = i * rows_per_task;
				idx_t row_end = std::min((i + 1) * rows_per_task - 1, partition_info.estimated_cardinality);

				// For last task, extend to include any remainder
				if (i == num_tasks - 1) {
					row_end = partition_info.estimated_cardinality;
				}

				// Create SQL with rowid filter
				string where_condition =
				    StringUtil::Format("rowid BETWEEN %llu AND %llu", static_cast<long long unsigned>(row_start),
				                       static_cast<long long unsigned>(row_end));
				task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);

				task.row_group_start = row_start / DEFAULT_ROW_GROUP_SIZE;
				task.row_group_end = row_end / DEFAULT_ROW_GROUP_SIZE;

				tasks.push_back(std::move(task));
			}
		} else {
			// Fallback: modulo-based partitioning
			DUCKDB_LOG_DEBUG(db_instance, "ℹ️  [PIPELINE] Using modulo-based partitioning fallback");

			for (idx_t i = 0; i < num_tasks; i++) {
				DistributedPipelineTask task;
				task.task_id = i;
				task.total_tasks = num_tasks;

				// Create SQL with modulo filter
				string where_condition =
				    StringUtil::Format("(rowid %% %llu) = %llu", static_cast<long long unsigned>(num_tasks),
				                       static_cast<long long unsigned>(i));
				task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);

				task.row_group_start = 0; // Unknown for modulo partitioning
				task.row_group_end = 0;

				tasks.push_back(std::move(task));
			}
		}

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔧 [PIPELINE] Created %llu distributed pipeline tasks",
		                                                 static_cast<long long unsigned>(tasks.size())));

	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("🔧 [PIPELINE] Failed to extract pipeline tasks: %s", ex.what()));

		// Fallback: create simple task-per-worker
		for (idx_t i = 0; i < num_workers; i++) {
			DistributedPipelineTask task;
			task.task_id = i;
			task.total_tasks = num_workers;

			// Use modulo partitioning as fallback
			string where_condition =
			    StringUtil::Format("(rowid %% %llu) = %llu", static_cast<long long unsigned>(num_workers),
			                       static_cast<long long unsigned>(i));
			task.task_sql = sql_generator.InjectWhereClause(base_sql, where_condition);
			tasks.push_back(std::move(task));
		}
	}

	return tasks;
}

} // namespace duckdb
