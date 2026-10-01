#include "server/driver/task_partitioner.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/query_plan_analyzer.hpp"

#include <algorithm>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

TaskPartitioner::TaskPartitioner(Connection &conn_p, QueryPlanAnalyzer &analyzer_p)
    : conn(conn_p), analyzer(analyzer_p) {
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
	if (num_workers == 0) {
		return {};
	}

	// Only partition direct DuckDB table scans; delegate other supported queries intact.
	auto statements = conn.ExtractStatements(base_sql);
	if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
		return {};
	}
	auto &statement = statements[0]->Cast<SelectStatement>();
	if (statement.node->type != QueryNodeType::SELECT_NODE) {
		return CreateSingleTask(base_sql);
	}
	auto &select = statement.node->Cast<SelectNode>();
	if (!select.from_table || select.from_table->type != TableReferenceType::BASE_TABLE ||
	    !select.cte_map.map.empty() || !select.modifiers.empty() || select.sample || select.from_table->sample ||
	    !select.from_table->column_name_alias.empty()) {
		return CreateSingleTask(base_sql);
	}
	auto *op = &logical_plan;
	bool has_aggregate = false;
	while (op->children.size() == 1) {
		has_aggregate |= op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY;
		op = op->children[0].get();
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return CreateSingleTask(base_sql);
	}
	auto table = op->Cast<LogicalGet>().GetTable();
	auto &table_ref = select.from_table->Cast<BaseTableRef>();
	if (!table || !table->IsDuckTable() || !StringUtil::CIEquals(table_ref.table_name, table->name)) {
		return CreateSingleTask(base_sql);
	}
	// Resolve the source on the driver so workers do not depend on their default catalog.
	table_ref.catalog_name = table->catalog.GetName();
	table_ref.schema_name = table->schema.name;
	const string task_sql = statement.ToString();
	// Complete aggregate queries execute intact on one worker.
	if (has_aggregate) {
		return CreateSingleTask(task_sql);
	}

	// Extract row group information for DuckDB-aligned partitioning
	// If reliable rowid bounds are unavailable, delegate instead of using modulo-based partitioning.
	auto row_group_info = analyzer.ExtractRowGroupInfo(logical_plan);
	if (!row_group_info.valid || row_group_info.total_row_groups == 0) {
		return CreateSingleTask(task_sql);
	}
	// Give each worker one contiguous run of whole row groups. Rowid pruning is row-group granular, so splitting a
	// row group makes every task read all of it; with static assignment, extra tasks per worker only break locality.
	// Tables with fewer row groups than workers leave the remaining workers idle.
	const idx_t num_tasks = std::min(num_workers, row_group_info.total_row_groups);
	const idx_t groups_per_task = row_group_info.total_row_groups / num_tasks;
	const idx_t remainder = row_group_info.total_row_groups % num_tasks;
	const string rowid =
	    ColumnRefExpression("rowid", table_ref.alias.empty() ? table_ref.table_name : table_ref.alias).ToString();
	vector<DistributedPipelineTask> tasks;
	tasks.reserve(num_tasks);
	for (idx_t task_idx = 0; task_idx < num_tasks; ++task_idx) {
		// Calculate which row groups this task processes; the first `remainder` tasks take one extra.
		const idx_t rg_start = task_idx * groups_per_task + std::min(task_idx, remainder);
		const idx_t rg_end = rg_start + groups_per_task + (task_idx < remainder ? 1 : 0);
		// Use actual storage starts instead of multiplying by an approximate row-group size.
		const idx_t row_start = row_group_info.row_group_starts[rg_start];
		const idx_t row_end = rg_end == row_group_info.total_row_groups ? row_group_info.rowid_end
		                                                                : row_group_info.row_group_starts[rg_end];
		DistributedPipelineTask task;
		task.task_id = task_idx;
		task.total_tasks = num_tasks;
		task.row_group_start = rg_start;
		task.row_group_end = rg_end - 1;
		// Create SQL with row group-aligned half-open rowid filter.
		task.task_sql = PartitionSQLGenerator::InjectWhereClause(
		    task_sql, StringUtil::Format("%s >= %llu AND %s < %llu", rowid, row_start, rowid, row_end));
		tasks.emplace_back(std::move(task));
	}
	return tasks;
}

} // namespace duckdb
