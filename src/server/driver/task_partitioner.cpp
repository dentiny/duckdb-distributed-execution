#include "server/driver/task_partitioner.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/query_utils.hpp"

#include <algorithm>
#include <functional>
#include <optional>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

bool HasEquiJoin(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		const auto &join = op.Cast<LogicalComparisonJoin>();
		for (const auto &condition : join.conditions) {
			if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
				return true;
			}
		}
	}
	for (const auto &child : op.children) {
		if (HasEquiJoin(*child)) {
			return true;
		}
	}
	return false;
}

bool MatchesRef(const BaseTableRef &ref, const TableCatalogEntry &table) {
	if (!StringUtil::CIEquals(ref.table_name, table.name)) {
		return false;
	}
	if (!ref.catalog_name.empty()) {
		return StringUtil::CIEquals(ref.catalog_name, table.catalog.GetName()) &&
		       (ref.schema_name.empty() || StringUtil::CIEquals(ref.schema_name, table.schema.name));
	}
	// DuckDB resolves a two-part name as either schema.table or catalog.table.
	return ref.schema_name.empty() || StringUtil::CIEquals(ref.schema_name, table.schema.name) ||
	       StringUtil::CIEquals(ref.schema_name, table.catalog.GetName());
}

void QualifyRef(BaseTableRef &ref, const TableCatalogEntry &table) {
	ref.catalog_name = table.catalog.GetName();
	ref.schema_name = table.schema.name;
}

void FlattenConjunction(const ParsedExpression &expression, ExpressionType type,
                        vector<const ParsedExpression *> &terms) {
	if (expression.GetExpressionType() == type && expression.GetExpressionClass() == ExpressionClass::CONJUNCTION) {
		for (const auto &child : expression.Cast<ConjunctionExpression>().children) {
			FlattenConjunction(*child, type, terms);
		}
	} else {
		terms.push_back(&expression);
	}
}

bool ReferencesTable(const ColumnRefExpression &column, const BaseTableRef &target,
                     const TableCatalogEntry &target_table, const BaseTableRef &other,
                     const TableCatalogEntry &other_table) {
	if (column.column_names.size() == 1) {
		const auto &name = column.column_names[0];
		return target_table.ColumnExists(name) && !other_table.ColumnExists(name);
	}
	if (column.column_names.size() != 2) {
		return false;
	}
	const auto &qualifier = column.column_names[0];
	const auto &target_name = target.alias.empty() ? target.table_name : target.alias;
	const auto &other_name = other.alias.empty() ? other.table_name : other.alias;
	return StringUtil::CIEquals(qualifier, target_name) && !StringUtil::CIEquals(qualifier, other_name) &&
	       target_table.ColumnExists(column.column_names[1]);
}

// If every OR branch compares the same replicated-table column to a constant, those comparisons
// form an implied filter. Otherwise keep the original WHERE without an extra filter.
unique_ptr<ParsedExpression> ImpliedEqualityFilter(const ParsedExpression &where_clause, const BaseTableRef &target,
                                                   const TableCatalogEntry &target_table, const BaseTableRef &other,
                                                   const TableCatalogEntry &other_table) {
	if (where_clause.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return nullptr;
	}
	vector<const ParsedExpression *> branches;
	FlattenConjunction(where_clause, ExpressionType::CONJUNCTION_OR, branches);
	vector<unique_ptr<ParsedExpression>> implied;
	string chosen_column;
	for (auto *branch : branches) {
		vector<const ParsedExpression *> terms;
		FlattenConjunction(*branch, ExpressionType::CONJUNCTION_AND, terms);
		const ParsedExpression *chosen = nullptr;
		for (auto *term : terms) {
			if (term->GetExpressionClass() != ExpressionClass::COMPARISON ||
			    term->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &comparison = term->Cast<ComparisonExpression>();
			const ParsedExpression *column = nullptr;
			if (comparison.left->GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			    comparison.right->GetExpressionClass() == ExpressionClass::CONSTANT) {
				column = comparison.left.get();
			} else if (comparison.right->GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			           comparison.left->GetExpressionClass() == ExpressionClass::CONSTANT) {
				column = comparison.right.get();
			}
			if (!column ||
			    !ReferencesTable(column->Cast<ColumnRefExpression>(), target, target_table, other, other_table)) {
				continue;
			}
			const auto &name = column->Cast<ColumnRefExpression>().GetColumnName();
			if (chosen_column.empty() || StringUtil::CIEquals(chosen_column, name)) {
				chosen_column = name;
				chosen = term;
				break;
			}
		}
		if (!chosen) {
			return nullptr;
		}
		implied.push_back(chosen->Copy());
	}
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(implied));
}

struct JoinPartitionInfo {
	LogicalGet *left_scan;
	LogicalGet *right_scan;
	bool partition_left;
	QueryPlanAnalyzer::RowGroupPartitionInfo row_group_info;
};

// A worker reads one rowid range from one table and all rows from the other. Require exactly two
// distinct native scans that match the SQL references before rewriting either table.
std::optional<JoinPartitionInfo> GetJoinPartitionInfo(LogicalOperator &logical_plan, const SelectNode &select,
                                                      QueryPlanAnalyzer &analyzer) {
	if (select.from_table->sample || !select.from_table->column_name_alias.empty()) {
		return std::nullopt;
	}
	const auto &join = select.from_table->Cast<JoinRef>();
	if (!join.alias.empty() || !HasEquiJoin(logical_plan)) {
		return std::nullopt;
	}
	const auto &left = join.left->Cast<BaseTableRef>();
	const auto &right = join.right->Cast<BaseTableRef>();
	if (left.sample || right.sample || left.at_clause || right.at_clause || !left.column_name_alias.empty() ||
	    !right.column_name_alias.empty() || StringUtil::CIEquals(left.table_name, right.table_name)) {
		return std::nullopt;
	}
	// Confirm that both parsed references resolve to the tables in the optimized plan.
	vector<LogicalGet *> scans;
	std::function<void(LogicalOperator &)> collect = [&](LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			scans.push_back(&op.Cast<LogicalGet>());
		}
		for (const auto &child : op.children) {
			collect(*child);
		}
	};
	collect(logical_plan);
	if (scans.size() != 2) {
		return std::nullopt;
	}
	LogicalGet *left_scan = nullptr;
	LogicalGet *right_scan = nullptr;
	for (auto *scan : scans) {
		auto table = scan->GetTable();
		if (!table || !table->IsDuckTable() || table->temporary) {
			return std::nullopt;
		}
		if (MatchesRef(left, *table)) {
			if (left_scan) {
				return std::nullopt;
			}
			left_scan = scan;
		} else if (MatchesRef(right, *table)) {
			if (right_scan) {
				return std::nullopt;
			}
			right_scan = scan;
		} else {
			return std::nullopt;
		}
	}
	if (!left_scan || !right_scan) {
		return std::nullopt;
	}
	auto left_groups = analyzer.ExtractRowGroupInfo(*left_scan);
	auto right_groups = analyzer.ExtractRowGroupInfo(*right_scan);
	if (!left_groups.valid || !right_groups.valid) {
		return std::nullopt;
	}
	// Partition the larger side so each worker repeats the smaller scan.
	// Row-group count approximates input size.
	const bool partition_left = left_groups.total_row_groups >= right_groups.total_row_groups;
	return JoinPartitionInfo {.left_scan = left_scan,
	                          .right_scan = right_scan,
	                          .partition_left = partition_left,
	                          .row_group_info = partition_left ? std::move(left_groups) : std::move(right_groups)};
}

} // namespace

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

bool TaskPartitioner::CanPartitionJoin(LogicalOperator &logical_plan, const SelectStatement &statement,
                                       string &qualified_sql, QueryPlanAnalyzer::QueryAnalysis &analysis) {
	if (!IsSimplePartitionedJoin(statement)) {
		return false;
	}
	auto info = GetJoinPartitionInfo(logical_plan, statement.node->Cast<SelectNode>(), analyzer);
	// One row group yields one task, so there is no Join work to spread across workers.
	if (!info || info->row_group_info.total_row_groups < 2) {
		return false;
	}
	// The executor rebinds this SQL on another connection, so keep the client's bound table identities.
	auto copy = statement.Copy();
	auto &join = copy->Cast<SelectStatement>().node->Cast<SelectNode>().from_table->Cast<JoinRef>();
	QualifyRef(join.left->Cast<BaseTableRef>(), *info->left_scan->GetTable());
	QualifyRef(join.right->Cast<BaseTableRef>(), *info->right_scan->GetTable());
	analysis = QueryPlanAnalyzer::AnalyzeQuery(logical_plan, copy->Cast<SelectStatement>());
	if (!analysis.supports_partitioned_aggregation) {
		return false;
	}
	qualified_sql = copy->ToString();
	return true;
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
	if (!select.from_table || !select.cte_map.map.empty() || !select.modifiers.empty() || select.sample ||
	    select.from_table->sample || !select.from_table->column_name_alias.empty()) {
		return CreateSingleTask(base_sql);
	}
	BaseTableRef *partition_ref = nullptr;
	QueryPlanAnalyzer::RowGroupPartitionInfo row_group_info;
	if (select.from_table->type == TableReferenceType::BASE_TABLE) {
		auto *op = &logical_plan;
		while (op->children.size() == 1) {
			op = op->children[0].get();
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return CreateSingleTask(base_sql);
		}
		auto table = op->Cast<LogicalGet>().GetTable();
		auto &ref = select.from_table->Cast<BaseTableRef>();
		if (!table || !table->IsDuckTable() || ref.at_clause || !MatchesRef(ref, *table)) {
			return CreateSingleTask(base_sql);
		}
		ref.catalog_name = table->catalog.GetName();
		ref.schema_name = table->schema.name;
		partition_ref = &ref;
		row_group_info = analyzer.ExtractRowGroupInfo(op->Cast<LogicalGet>());
	} else if (select.from_table->type == TableReferenceType::JOIN) {
		// Give workers disjoint ranges of one input; each repeats the other input and joins locally.
		// This avoids a shuffle and keeps every matching pair in exactly one task.
		if (!IsSimplePartitionedJoin(statement)) {
			return CreateSingleTask(base_sql);
		}
		auto info = GetJoinPartitionInfo(logical_plan, select, analyzer);
		if (!info) {
			return CreateSingleTask(base_sql);
		}
		auto &join = select.from_table->Cast<JoinRef>();
		auto &left = join.left->Cast<BaseTableRef>();
		auto &right = join.right->Cast<BaseTableRef>();
		auto left_table = info->left_scan->GetTable();
		auto right_table = info->right_scan->GetTable();
		// Use bound names so workers resolve the same tables as the driver.
		QualifyRef(left, *left_table);
		QualifyRef(right, *right_table);
		// Only this reference receives the rowid predicate below; the other stays unpartitioned.
		partition_ref = info->partition_left ? &left : &right;
		row_group_info = std::move(info->row_group_info);
		if (select.where_clause) {
			auto &dimension = info->partition_left ? right : left;
			auto &fact = info->partition_left ? left : right;
			auto dimension_table = info->partition_left ? right_table : left_table;
			auto fact_table = info->partition_left ? left_table : right_table;
			// Each OR branch may restrict the replicated input to a constant on the same column. Their
			// disjunction is already implied by WHERE; adding it lets DuckDB filter that input before
			// the Join on every worker without changing the result.
			auto implied = ImpliedEqualityFilter(*select.where_clause, dimension, *dimension_table, fact, *fact_table);
			if (implied) {
				select.where_clause = make_uniq<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(select.where_clause), std::move(implied));
			}
		}
	} else {
		return CreateSingleTask(base_sql);
	}
	const string task_sql = statement.ToString();

	// If reliable rowid bounds are unavailable, delegate instead of using modulo-based partitioning.
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
	    ColumnRefExpression("rowid", partition_ref->alias.empty() ? partition_ref->table_name : partition_ref->alias)
	        .ToString();
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
