#include "utils/sql_render_utils.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/common/sql_identifier.hpp"
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
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

namespace {

bool IsPushableAggregate(const Identifier &name) {
	return name == "count_star" || name == "count" || name == "sum" || name == "min" || name == "max" || name == "avg";
}

// Bindings visible to the aggregate: scan columns, and expressions of projections between the scans and the aggregate.
struct RemoteScope {
	column_binding_map_t<string> columns;
	column_binding_map_t<optional_ptr<const Expression>> projections;
	// The column a table filter's expression refers to as reference 0.
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

// Renders a function call, which includes casts, comparisons and BETWEEN, like RenderExpression.
string RenderFunction(const RemoteScope &scope, const BoundFunctionExpression &function) {
	auto &type = function.GetReturnType();
	if (BoundCastExpression::IsCast(function)) {
		// Casts between time zone aware and naive types depend on the session's time zone.
		if (DependsOnTimeZone(BoundCastExpression::SourceType(function))) {
			return "";
		}
		auto child = RenderExpression(scope, BoundCastExpression::Child(function));
		if (child.empty()) {
			return "";
		}
		return StringUtil::Format("%s(%s AS %s)", BoundCastExpression::IsTryCast(function) ? "TRY_CAST" : "CAST", child,
		                          type.ToString());
	}
	if (function.GetExpressionType() == ExpressionType::COMPARE_BETWEEN &&
	    BoundBetweenExpression::HasValidBindData(function)) {
		// BETWEEN evaluates as the conjunction of its two comparisons.
		auto input = RenderExpression(scope, BoundBetweenExpression::Input(function));
		auto lower = RenderExpression(scope, BoundBetweenExpression::LowerBound(function));
		auto upper = RenderExpression(scope, BoundBetweenExpression::UpperBound(function));
		if (input.empty() || lower.empty() || upper.empty()) {
			return "";
		}
		return StringUtil::Format("(%s %s %s AND %s %s %s)", input,
		                          BoundBetweenExpression::LowerInclusive(function) ? ">=" : ">", lower, input,
		                          BoundBetweenExpression::UpperInclusive(function) ? "<=" : "<", upper);
	}
	if (BoundComparisonExpression::IsComparison(function)) {
		const auto op = ExpressionTypeToOperator(function.GetExpressionType());
		auto left = RenderExpression(scope, BoundComparisonExpression::Left(function));
		auto right = RenderExpression(scope, BoundComparisonExpression::Right(function));
		if (op.empty() || left.empty() || right.empty()) {
			return "";
		}
		return StringUtil::Format("(%s %s %s)", left, op, right);
	}
	auto &bound = function.Function();
	// Optimizer-internal functions, such as compressed materialization's or table filters', cannot be bound from SQL.
	if (bound.GetStability() != FunctionStability::CONSISTENT || bound.GetName().StartsWith("__internal_")) {
		return "";
	}
	vector<string> args;
	if (!RenderChildren(scope, function.GetChildren(), args)) {
		return "";
	}
	// Quoting also calls operators by name, e.g. "+"(a, b).
	return StringUtil::Format("%s(%s)", SQLQuotedIdentifier::ToString(bound.GetName()), StringUtil::Join(args, ", "));
}

// Renders `expr` as SQL on the remote table, or returns an empty string if it cannot be translated with identical
// semantics.
string RenderExpression(const RemoteScope &scope, const Expression &expr) {
	auto &type = expr.GetReturnType();
	// A table filter compares its column with constants as instants, whatever the session's time zone, but other
	// expressions of time zone aware types may depend on it.
	const bool filter_leaf =
	    !scope.filter_column.empty() && (expr.GetExpressionClass() == ExpressionClass::BOUND_REF ||
	                                     expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
	                                     expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT);
	if (!SupportsRemoteFilterPushdown(type) || (DependsOnTimeZone(type) && !filter_leaf)) {
		return "";
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &binding = expr.Cast<BoundColumnRefExpression>().Binding();
		auto column = scope.columns.find(binding);
		if (column != scope.columns.end()) {
			return column->second;
		}
		auto projection = scope.projections.find(binding);
		if (projection != scope.projections.end()) {
			return RenderExpression(scope, *projection->second);
		}
		// Table filters may refer to their column with a column reference instead of reference 0.
		return scope.filter_column;
	}
	case ExpressionClass::BOUND_REF:
		return expr.Cast<BoundReferenceExpression>().Index() == 0 ? scope.filter_column : "";
	case ExpressionClass::BOUND_CONSTANT: {
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ && !value.IsNull()) {
			// The SQL string of the value is in the session's time zone, without an offset the server could read.
			auto utc = Timestamp::ToString(timestamp_t(value.GetValue<timestamp_tz_t>().value));
			return StringUtil::Format("CAST('%s+00' AS %s)", utc, type.ToString());
		}
		return StringUtil::Format("CAST(%s AS %s)", value.ToSQLString(), type.ToString());
	}
	case ExpressionClass::BOUND_FUNCTION:
		return RenderFunction(scope, expr.Cast<BoundFunctionExpression>());
	case ExpressionClass::BOUND_CONJUNCTION: {
		vector<string> children;
		if (!RenderChildren(scope, expr.Cast<BoundConjunctionExpression>().GetChildren(), children)) {
			return "";
		}
		return StringUtil::Format(
		    "(%s)",
		    StringUtil::Join(children, StringUtil::Format(" %s ", ExpressionTypeToOperator(expr.GetExpressionType()))));
	}
	case ExpressionClass::BOUND_OPERATOR: {
		vector<string> children;
		if (!RenderChildren(scope, expr.Cast<BoundOperatorExpression>().GetChildren(), children)) {
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
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		string sql = "CASE";
		for (const auto &check : case_expr.CaseChecks()) {
			auto when = RenderExpression(scope, *check.when_expr);
			auto then = RenderExpression(scope, *check.then_expr);
			if (when.empty() || then.empty()) {
				return "";
			}
			sql += StringUtil::Format(" WHEN %s THEN %s", when, then);
		}
		auto else_sql = RenderExpression(scope, case_expr.Else());
		return else_sql.empty() ? "" : StringUtil::Format("%s ELSE %s END", sql, else_sql);
	}
	default:
		return "";
	}
}

// Renders a table filter's expression into sql, which is empty if the filter accepts every row, e.g. an optional
// filter that only prunes by statistics. Returns false if it cannot be translated.
bool TryRenderFilter(const RemoteScope &scope, const Expression &expr, string &sql) {
	sql.clear();
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &name = expr.Cast<BoundFunctionExpression>().Function().GetName();
		if (name == OptionalFilterScalarFun::NAME || name == SelectivityOptionalFilterScalarFun::NAME) {
			return true;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		const bool is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		vector<string> predicates;
		for (const auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			string predicate;
			if (!TryRenderFilter(scope, *child, predicate)) {
				return false;
			}
			if (!predicate.empty()) {
				predicates.emplace_back(std::move(predicate));
			} else if (!is_and) {
				// A branch accepting every row makes the whole disjunction accept every row.
				return true;
			}
		}
		if (!predicates.empty()) {
			sql = StringUtil::Format("(%s)", StringUtil::Join(predicates, is_and ? " AND " : " OR "));
		}
		return true;
	}
	sql = RenderExpression(scope, expr);
	return !sql.empty();
}

// Renders a single-column table filter on column, or returns false if it cannot be translated.
bool TryRenderTableFilter(const TableFilter &filter, const string &column, string &sql) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &expression_filter = filter.Cast<ExpressionFilter>();
	if (!expression_filter.column_indexes.empty()) {
		return false;
	}
	RemoteScope scope;
	scope.filter_column = column;
	return TryRenderFilter(scope, *expression_filter.expr, sql);
}

string RenderAggregate(const RemoteScope &scope, const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return "";
	}
	auto &aggregate = expr.Cast<BoundAggregateExpression>();
	// Statistics propagation turns `sum` into `sum_no_overflow` once overflow is ruled out, which `sum` also computes.
	const auto &function_name = aggregate.Function().GetName();
	const Identifier name = function_name == "sum_no_overflow" ? Identifier("sum") : function_name;
	if (aggregate.IsDistinct() || aggregate.GetFilter() || aggregate.GetOrderBys() || !IsPushableAggregate(name)) {
		return "";
	}
	if (name == "count_star") {
		return "count(*)";
	}
	if (aggregate.GetChildren().size() != 1) {
		return "";
	}
	auto arg = RenderExpression(scope, *aggregate.GetChildren()[0]);
	return arg.empty() ? "" : StringUtil::Format("%s(%s)", name.GetIdentifierName(), arg);
}

// Makes the expressions of `projection` visible to expressions above it.
void AddProjection(const LogicalProjection &projection, RemoteScope &scope) {
	for (idx_t idx = 0; idx < projection.expressions.size(); ++idx) {
		scope.projections.emplace(ColumnBinding(projection.table_index, ProjectionIndex(idx)),
		                          projection.expressions[idx].get());
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
		scope.columns.emplace(ColumnBinding(get.table_index, ProjectionIndex(idx)), std::move(column));
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
		result->types.emplace_back(group->GetReturnType());
		result->names.emplace_back(group->GetName());
	}
	for (auto &expr : aggregate.expressions) {
		auto sql = RenderAggregate(scope, *expr);
		if (sql.empty()) {
			return nullptr;
		}
		select_list.emplace_back(std::move(sql));
		result->types.emplace_back(expr->GetReturnType());
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
	string sql;
	if (!TryRenderTableFilter(filter, column, sql)) {
		throw InternalException("Distributed table scan cannot push down a table filter on %s", column);
	}
	return sql;
}

string GetColumnSQL(const TableCatalogEntry &table, column_t column_id, LogicalType &type) {
	if (IsVirtualColumn(column_id)) {
		const auto virtual_columns = table.GetVirtualColumns();
		auto entry = virtual_columns.find(column_id);
		if (entry == virtual_columns.end()) {
			throw InternalException("Table %s has no virtual column %llu", table.name, column_id);
		}
		type = entry->second.type;
		return SQLIdentifier::ToString(entry->second.name);
	}
	auto &column = table.GetColumn(LogicalIndex(column_id));
	type = column.Type();
	return SQLIdentifier::ToString(column.Name());
}

namespace {

// Renders the filters of get on table as SQL predicates, or returns false if one cannot be translated.
bool RenderScanFilters(const LogicalGet &get, const TableCatalogEntry &table, vector<string> &predicates) {
	// Logical table filters are keyed by the position of their column in the scan.
	auto &column_ids = get.GetColumnIds();
	for (const auto &entry : get.table_filters) {
		LogicalType type;
		auto column = GetColumnSQL(table, column_ids[entry.GetIndex()].GetPrimaryIndex(), type);
		string predicate;
		if (!SupportsRemoteFilterPushdown(type) || !TryRenderTableFilter(entry.Filter(), column, predicate)) {
			return false;
		}
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

void ReplaceAggregateBindings(const LogicalAggregate &aggregate, TableIndex table_index,
                              vector<ReplacementBinding> &replacements) {
	const auto group_count = aggregate.groups.size();
	for (idx_t idx = 0; idx < group_count; ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.group_index, ProjectionIndex(idx)),
		                          ColumnBinding(table_index, ProjectionIndex(idx)));
	}
	for (idx_t idx = 0; idx < aggregate.expressions.size(); ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.aggregate_index, ProjectionIndex(idx)),
		                          ColumnBinding(table_index, ProjectionIndex(group_count + idx)));
	}
}

} // namespace duckdb
