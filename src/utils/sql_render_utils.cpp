#include "utils/sql_render_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
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

// Bindings visible to the aggregate: scan columns, and expressions of an optional projection above the scan.
struct RemoteScope {
	column_binding_map_t<string> columns;
	column_binding_map_t<optional_ptr<const Expression>> projections;
};

// Renders `expr` as SQL on the remote table, or returns an empty string if it cannot be translated with identical
// semantics.
string RenderExpression(const RemoteScope &scope, const Expression &expr) {
	// Results on TIMESTAMPTZ and TIMETZ may depend on client settings such as TimeZone, which the server lacks.
	auto &type = expr.return_type;
	if (!SupportsRemoteFilterPushdown(type) || type.id() == LogicalTypeId::TIMESTAMP_TZ ||
	    type.id() == LogicalTypeId::TIME_TZ) {
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
		if (function.function.GetStability() != FunctionStability::CONSISTENT) {
			return "";
		}
		vector<string> args;
		for (auto &child : function.children) {
			auto arg = RenderExpression(scope, *child);
			if (arg.empty()) {
				return "";
			}
			args.emplace_back(std::move(arg));
		}
		// Quoting also calls operators by name, e.g. "+"(a, b).
		return StringUtil::Format("%s(%s)", KeywordHelper::WriteQuoted(function.function.name, '"'),
		                          StringUtil::Join(args, ", "));
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

} // namespace

bool SupportsRemoteFilterPushdown(const LogicalType &type) {
	return type.id() != LogicalTypeId::ENUM && !type.HasAlias() && !type.IsNested();
}

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

bool RenderScanFilters(const LogicalGet &get, const RemoteColumnFunction &get_column, vector<string> &predicates) {
	for (auto &entry : get.table_filters.filters) {
		LogicalType type;
		auto column = get_column(entry.first, type);
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

string RenderAggregateQuery(LogicalAggregate &aggregate, const RemoteColumnFunction &get_column,
                            const string &table_name, vector<LogicalType> &types, vector<string> &names) {
	if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty()) {
		return "";
	}
	if (!aggregate.grouping_sets.empty() && aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
		return "";
	}
	auto get = GetAggregateScan(aggregate);
	if (!get || get->extra_info.sample_options) {
		return "";
	}
	RemoteScope scope;
	if (aggregate.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = aggregate.children[0]->Cast<LogicalProjection>();
		for (idx_t idx = 0; idx < projection.expressions.size(); ++idx) {
			scope.projections.emplace(ColumnBinding(projection.table_index, idx), projection.expressions[idx].get());
		}
	}
	auto &column_ids = get->GetColumnIds();
	for (idx_t idx = 0; idx < column_ids.size(); ++idx) {
		auto column_id = column_ids[idx].GetPrimaryIndex();
		if (column_id == COLUMN_IDENTIFIER_EMPTY || column_ids[idx].HasChildren()) {
			continue;
		}
		LogicalType type;
		scope.columns.emplace(ColumnBinding(get->table_index, idx), get_column(column_id, type));
	}

	vector<string> predicates;
	if (!RenderScanFilters(*get, get_column, predicates)) {
		return "";
	}

	vector<string> select_list;
	vector<string> group_by;
	for (auto &group : aggregate.groups) {
		auto sql = RenderExpression(scope, *group);
		if (sql.empty()) {
			return "";
		}
		select_list.emplace_back(std::move(sql));
		group_by.emplace_back(std::to_string(group_by.size() + 1));
		types.emplace_back(group->return_type);
		names.emplace_back(group->GetName());
	}
	for (auto &expr : aggregate.expressions) {
		auto sql = RenderAggregate(scope, *expr);
		if (sql.empty()) {
			return "";
		}
		select_list.emplace_back(std::move(sql));
		types.emplace_back(expr->return_type);
		names.emplace_back(expr->GetName());
	}

	auto query = StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), table_name);
	if (!predicates.empty()) {
		query += " WHERE " + StringUtil::Join(predicates, " AND ");
	}
	if (!group_by.empty()) {
		query += " GROUP BY " + StringUtil::Join(group_by, ", ");
	}
	return query;
}

} // namespace duckdb
