#include "utils/sql_render_utils.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/list.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

namespace {

bool IsPushableAggregate(const string &name) {
	return name == "count_star" || name == "count" || name == "sum" || name == "min" || name == "max" || name == "avg";
}

// Bindings visible to the aggregate: scan columns, and expressions of projections between the scans and the aggregate.
struct RemoteScope {
	column_binding_map_t<string> columns;
	column_binding_map_t<optional_ptr<const Expression>> projections;
	// The column an expression table filter refers to by reference.
	string filter_column;
};

string RenderExpression(const RemoteScope &scope, const Expression &expr);

// Renders each of `children` into `result`, or returns false if one cannot be translated.
bool RenderChildren(const RemoteScope &scope, const vector<unique_ptr<Expression>> &children, vector<string> &result) {
	result.reserve(result.size() + children.size());
	for (const auto &child : children) {
		auto sql = RenderExpression(scope, *child);
		if (sql.empty()) {
			return false;
		}
		result.emplace_back(std::move(sql));
	}
	return true;
}

// Renders `expr` as SQL on the remote table, or returns an empty string if it cannot be translated with identical
// semantics.
string RenderExpression(const RemoteScope &scope, const Expression &expr) {
	auto &type = expr.return_type;
	if (!SupportsRemoteFilterPushdown(type) || DependsOnTimeZone(type)) {
		return "";
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &binding = expr.Cast<BoundColumnRefExpression>().binding;
		auto column = scope.columns.find(binding);
		if (column != scope.columns.end()) {
			return column->second;
		}
		auto projection = scope.projections.find(binding);
		return projection == scope.projections.end() ? "" : RenderExpression(scope, *projection->second);
	}
	case ExpressionClass::BOUND_REF:
		return scope.filter_column;
	case ExpressionClass::BOUND_CONSTANT: {
		auto &value = expr.Cast<BoundConstantExpression>().value;
		return StringUtil::Format("CAST(%s AS %s)", value.ToSQLString(), type.ToString());
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		auto child = RenderExpression(scope, *cast.child);
		if (child.empty()) {
			return "";
		}
		return StringUtil::Format("%s(%s AS %s)", cast.try_cast ? "TRY_CAST" : "CAST", child, type.ToString());
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		// Optimizer-internal functions, such as compressed materialization's, cannot be bound from SQL.
		if (function.function.GetStability() != FunctionStability::CONSISTENT ||
		    StringUtil::StartsWith(function.function.name, "__internal_")) {
			return "";
		}
		vector<string> args;
		if (!RenderChildren(scope, function.children, args)) {
			return "";
		}
		// Quoting also calls operators by name, e.g. "+"(a, b).
		return StringUtil::Format("%s(%s)", KeywordHelper::WriteQuoted(function.function.name, '"'),
		                          StringUtil::Join(args, ", "));
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comparison = expr.Cast<BoundComparisonExpression>();
		const auto op = ExpressionTypeToOperator(expr.GetExpressionType());
		auto left = RenderExpression(scope, *comparison.left);
		auto right = RenderExpression(scope, *comparison.right);
		if (op.empty() || left.empty() || right.empty()) {
			return "";
		}
		return StringUtil::Format("(%s %s %s)", left, op, right);
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		vector<string> children;
		if (!RenderChildren(scope, expr.Cast<BoundConjunctionExpression>().children, children)) {
			return "";
		}
		return StringUtil::Format(
		    "(%s)",
		    StringUtil::Join(children, StringUtil::Format(" %s ", ExpressionTypeToOperator(expr.GetExpressionType()))));
	}
	case ExpressionClass::BOUND_OPERATOR: {
		vector<string> children;
		if (!RenderChildren(scope, expr.Cast<BoundOperatorExpression>().children, children)) {
			return "";
		}
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_NOT:
			return StringUtil::Format("(NOT %s)", children[0]);
		case ExpressionType::OPERATOR_IS_NULL:
			return StringUtil::Format("(%s IS NULL)", children[0]);
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return StringUtil::Format("(%s IS NOT NULL)", children[0]);
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN: {
			auto input = std::move(children[0]);
			children.erase(children.begin());
			return StringUtil::Format("(%s %s (%s))", input,
			                          expr.GetExpressionType() == ExpressionType::COMPARE_IN ? "IN" : "NOT IN",
			                          StringUtil::Join(children, ", "));
		}
		default:
			return "";
		}
	}
	case ExpressionClass::BOUND_BETWEEN: {
		// BETWEEN evaluates as the conjunction of its two comparisons.
		auto &between = expr.Cast<BoundBetweenExpression>();
		auto input = RenderExpression(scope, *between.input);
		auto lower = RenderExpression(scope, *between.lower);
		auto upper = RenderExpression(scope, *between.upper);
		if (input.empty() || lower.empty() || upper.empty()) {
			return "";
		}
		return StringUtil::Format("(%s %s %s AND %s %s %s)", input, between.lower_inclusive ? ">=" : ">", lower, input,
		                          between.upper_inclusive ? "<=" : "<", upper);
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		string sql = "CASE";
		for (const auto &check : case_expr.case_checks) {
			auto when = RenderExpression(scope, *check.when_expr);
			auto then = RenderExpression(scope, *check.then_expr);
			if (when.empty() || then.empty()) {
				return "";
			}
			sql += StringUtil::Format(" WHEN %s THEN %s", when, then);
		}
		auto else_sql = RenderExpression(scope, *case_expr.else_expr);
		return else_sql.empty() ? "" : StringUtil::Format("%s ELSE %s END", sql, else_sql);
	}
	default:
		return "";
	}
}

string RenderAggregate(const RemoteScope &scope, const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return "";
	}
	auto &aggregate = expr.Cast<BoundAggregateExpression>();
	// Statistics propagation turns `sum` into `sum_no_overflow` once overflow is ruled out, which `sum` also computes.
	const string name = aggregate.function.name == "sum_no_overflow" ? "sum" : aggregate.function.name;
	if (aggregate.IsDistinct() || aggregate.filter || aggregate.order_bys || !IsPushableAggregate(name)) {
		return "";
	}
	if (name == "count_star") {
		return "count(*)";
	}
	if (aggregate.children.size() != 1) {
		return "";
	}
	auto arg = RenderExpression(scope, *aggregate.children[0]);
	return arg.empty() ? "" : StringUtil::Format("%s(%s)", name, arg);
}

// Whether `RemoteFilterToSQL` can translate `filter`.
bool IsRemoteFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::OPTIONAL_FILTER:
		return true;
	case TableFilterType::CONJUNCTION_AND:
		for (auto &child : filter.Cast<ConjunctionAndFilter>().child_filters) {
			if (!IsRemoteFilter(*child)) {
				return false;
			}
		}
		return true;
	case TableFilterType::CONJUNCTION_OR:
		for (auto &child : filter.Cast<ConjunctionOrFilter>().child_filters) {
			if (!IsRemoteFilter(*child)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

// Makes the expressions of `projection` visible to expressions above it.
void AddProjection(const LogicalProjection &projection, RemoteScope &scope) {
	for (idx_t idx = 0; idx < projection.expressions.size(); ++idx) {
		scope.projections.emplace(ColumnBinding(projection.table_index, idx), projection.expressions[idx].get());
	}
}

// Makes the columns get reads visible.
void AddScanColumns(const LogicalGet &get, const TableCatalogEntry &table, RemoteScope &scope) {
	auto &column_ids = get.GetColumnIds();
	for (idx_t idx = 0; idx < column_ids.size(); ++idx) {
		auto column_id = column_ids[idx].GetPrimaryIndex();
		if (column_id == COLUMN_IDENTIFIER_EMPTY || column_ids[idx].HasChildren()) {
			continue;
		}
		LogicalType type;
		auto column = GetColumnSQL(table, column_id, type);
		scope.columns.emplace(ColumnBinding(get.table_index, idx), std::move(column));
	}
}

// Renders `aggregate` as `SELECT <groups>, <aggregates> FROM <from> [WHERE <predicates>] [GROUP BY 1, ..., k]`, or
// returns nullptr if it cannot be translated with identical semantics.
unique_ptr<RemoteAggregateQuery> RenderGroupedQuery(const LogicalAggregate &aggregate, const RemoteScope &scope,
                                                    const string &from, const vector<string> &predicates) {
	if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty()) {
		return nullptr;
	}
	if (!aggregate.grouping_sets.empty() && aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
		return nullptr;
	}
	auto result = make_uniq<RemoteAggregateQuery>();
	vector<string> select_list;
	vector<string> group_by;
	for (auto &group : aggregate.groups) {
		auto sql = RenderExpression(scope, *group);
		if (sql.empty()) {
			return nullptr;
		}
		select_list.emplace_back(std::move(sql));
		group_by.emplace_back(std::to_string(group_by.size() + 1));
		result->types.emplace_back(group->return_type);
		result->names.emplace_back(group->GetName());
	}
	for (auto &expr : aggregate.expressions) {
		auto sql = RenderAggregate(scope, *expr);
		if (sql.empty()) {
			return nullptr;
		}
		select_list.emplace_back(std::move(sql));
		result->types.emplace_back(expr->return_type);
		result->names.emplace_back(expr->GetName());
	}
	result->sql = RenderSelectQuery(select_list, from, predicates);
	if (!group_by.empty()) {
		result->sql += " GROUP BY " + StringUtil::Join(group_by, ", ");
	}
	return result;
}

} // namespace

bool DependsOnTimeZone(const LogicalType &type) {
	return type.id() == LogicalTypeId::TIMESTAMP_TZ || type.id() == LogicalTypeId::TIME_TZ;
}

bool SupportsRemoteFilterPushdown(const LogicalType &type) {
	return type.id() != LogicalTypeId::ENUM && !type.HasAlias() && !type.IsNested();
}

string RemoteFilterToSQL(const TableFilter &filter, const string &column) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto &constant = constant_filter.constant;
		// A typed literal makes the server compare with the column type, e.g. FLOAT instead of DECIMAL.
		return StringUtil::Format("%s %s CAST(%s AS %s)", column,
		                          ExpressionTypeToOperator(constant_filter.comparison_type), constant.ToSQLString(),
		                          constant.type().ToString());
	}
	case TableFilterType::IS_NULL:
		return StringUtil::Format("%s IS NULL", column);
	case TableFilterType::IS_NOT_NULL:
		return StringUtil::Format("%s IS NOT NULL", column);
	case TableFilterType::CONJUNCTION_AND: {
		vector<string> predicates;
		for (auto &child : filter.Cast<ConjunctionAndFilter>().child_filters) {
			auto predicate = RemoteFilterToSQL(*child, column);
			if (!predicate.empty()) {
				predicates.emplace_back(std::move(predicate));
			}
		}
		if (predicates.empty()) {
			return "";
		}
		return StringUtil::Format("(%s)", StringUtil::Join(predicates, " AND "));
	}
	case TableFilterType::CONJUNCTION_OR: {
		vector<string> predicates;
		for (auto &child : filter.Cast<ConjunctionOrFilter>().child_filters) {
			auto predicate = RemoteFilterToSQL(*child, column);
			// An optional branch accepts every row, and so does the whole disjunction.
			if (predicate.empty()) {
				return "";
			}
			predicates.emplace_back(std::move(predicate));
		}
		return StringUtil::Format("(%s)", StringUtil::Join(predicates, " OR "));
	}
	case TableFilterType::OPTIONAL_FILTER:
		return "";
	default:
		throw InternalException("Distributed table scan cannot push down table filter %s", filter.ToString(column));
	}
}

string GetColumnSQL(const TableCatalogEntry &table, column_t column_id, LogicalType &type) {
	if (IsVirtualColumn(column_id)) {
		const auto virtual_columns = table.GetVirtualColumns();
		auto entry = virtual_columns.find(column_id);
		if (entry == virtual_columns.end()) {
			throw InternalException("Table %s has no virtual column %llu", table.name, column_id);
		}
		type = entry->second.type;
		return KeywordHelper::WriteOptionallyQuoted(entry->second.name);
	}
	auto &column = table.GetColumn(LogicalIndex(column_id));
	type = column.Type();
	return KeywordHelper::WriteOptionallyQuoted(column.Name());
}

namespace {

// Renders the filters of get on table as SQL predicates, or returns false if one cannot be translated.
bool RenderScanFilters(const LogicalGet &get, const TableCatalogEntry &table, vector<string> &predicates) {
	for (auto &entry : get.table_filters.filters) {
		LogicalType type;
		auto column = GetColumnSQL(table, entry.first, type);
		if (entry.second->filter_type == TableFilterType::EXPRESSION_FILTER) {
			RemoteScope scope;
			scope.filter_column = column;
			auto predicate = RenderExpression(scope, *entry.second->Cast<ExpressionFilter>().expr);
			if (predicate.empty()) {
				return false;
			}
			predicates.emplace_back(std::move(predicate));
			continue;
		}
		if (!SupportsRemoteFilterPushdown(type) || !IsRemoteFilter(*entry.second)) {
			return false;
		}
		auto predicate = RemoteFilterToSQL(*entry.second, column);
		if (!predicate.empty()) {
			predicates.emplace_back(std::move(predicate));
		}
	}
	return true;
}

} // namespace

string RenderSelectQuery(const vector<string> &select_list, const string &table_name,
                         const vector<string> &predicates) {
	auto sql = StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), table_name);
	if (!predicates.empty()) {
		sql += " WHERE " + StringUtil::Join(predicates, " AND ");
	}
	return sql;
}

optional_ptr<LogicalGet> GetAggregateScan(LogicalAggregate &aggregate) {
	reference<LogicalOperator> child = *aggregate.children[0];
	if (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		child = *child.get().children[0];
	}
	if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	return child.get().Cast<LogicalGet>();
}

unique_ptr<RemoteAggregateQuery> RenderAggregateQuery(LogicalAggregate &aggregate, const LogicalGet &get,
                                                      const TableCatalogEntry &table, const string &table_name) {
	RemoteScope scope;
	if (aggregate.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		AddProjection(aggregate.children[0]->Cast<LogicalProjection>(), scope);
	}
	AddScanColumns(get, table, scope);
	vector<string> predicates;
	if (!RenderScanFilters(get, table, predicates)) {
		return nullptr;
	}
	return RenderGroupedQuery(aggregate, scope, table_name, predicates);
}

void ReplaceAggregateBindings(const LogicalAggregate &aggregate, idx_t table_index,
                              vector<ReplacementBinding> &replacements) {
	const auto group_count = aggregate.groups.size();
	for (idx_t idx = 0; idx < group_count; ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.group_index, idx), ColumnBinding(table_index, idx));
	}
	for (idx_t idx = 0; idx < aggregate.expressions.size(); ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.aggregate_index, idx),
		                          ColumnBinding(table_index, group_count + idx));
	}
}

} // namespace duckdb
