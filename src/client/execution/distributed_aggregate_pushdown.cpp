#include "client/execution/distributed_aggregate_pushdown.hpp"

#include "client/execution/distributed_table_scan_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

bool IsPushableAggregate(const string &name) {
	return name == "count_star" || name == "count" || name == "sum" || name == "min" || name == "max" || name == "avg";
}

// Function and cast results on these types may depend on client settings such as TimeZone, which the server lacks.
bool IsRemoteSafeType(const LogicalType &type) {
	return SupportsRemoteFilterPushdown(type) && type.id() != LogicalTypeId::TIMESTAMP_TZ &&
	       type.id() != LogicalTypeId::TIME_TZ;
}

bool IsIdentifier(const string &name) {
	if (name.empty()) {
		return false;
	}
	for (auto c : name) {
		if (!StringUtil::CharacterIsAlphaNumeric(c) && c != '_') {
			return false;
		}
	}
	return true;
}

// Bindings visible to the aggregate: scan columns, and expressions of an optional projection above the scan.
struct RemoteScope {
	column_binding_map_t<string> columns;
	column_binding_map_t<optional_ptr<const Expression>> projections;
};

// Renders `expr` as SQL on the remote table, or returns an empty string if it cannot be translated with identical
// semantics.
string RenderExpression(const RemoteScope &scope, const Expression &expr) {
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
		if (!IsRemoteSafeType(value.type())) {
			return "";
		}
		return StringUtil::Format("CAST(%s AS %s)", value.ToSQLString(), value.type().ToString());
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		if (!IsRemoteSafeType(cast.child->return_type) || !IsRemoteSafeType(cast.return_type)) {
			return "";
		}
		auto child = RenderExpression(scope, *cast.child);
		if (child.empty()) {
			return "";
		}
		return StringUtil::Format("%s(%s AS %s)", cast.try_cast ? "TRY_CAST" : "CAST", child,
		                          cast.return_type.ToString());
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.function.GetStability() != FunctionStability::CONSISTENT ||
		    !IsRemoteSafeType(function.return_type)) {
			return "";
		}
		vector<string> args;
		for (auto &child : function.children) {
			if (!IsRemoteSafeType(child->return_type)) {
				return "";
			}
			auto arg = RenderExpression(scope, *child);
			if (arg.empty()) {
				return "";
			}
			args.emplace_back(std::move(arg));
		}
		auto &name = function.function.name;
		if (function.is_operator) {
			if (args.size() == 1) {
				return StringUtil::Format("(%s %s)", name, args[0]);
			}
			if (args.size() == 2) {
				return StringUtil::Format("(%s %s %s)", args[0], name, args[1]);
			}
			return "";
		}
		if (!IsIdentifier(name)) {
			return "";
		}
		return StringUtil::Format("%s(%s)", name, StringUtil::Join(args, ", "));
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
	auto &name = aggregate.function.name;
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

// Returns a remote scan computing `aggregate` on the server, or nullptr if the aggregate cannot be pushed down.
unique_ptr<LogicalOperator> TryPushdownAggregate(Binder &binder, LogicalAggregate &aggregate,
                                                 vector<ReplacementBinding> &replacements) {
	if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty()) {
		return nullptr;
	}
	if (!aggregate.grouping_sets.empty() && aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
		return nullptr;
	}
	RemoteScope scope;
	reference<LogicalOperator> child = *aggregate.children[0];
	if (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = child.get().Cast<LogicalProjection>();
		for (idx_t idx = 0; idx < projection.expressions.size(); ++idx) {
			scope.projections.emplace(ColumnBinding(projection.table_index, idx), projection.expressions[idx].get());
		}
		child = *projection.children[0];
	}
	if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = child.get().Cast<LogicalGet>();
	if (get.function.name != "distributed_scan" || get.extra_info.sample_options) {
		return nullptr;
	}
	auto &bind_data = get.bind_data->Cast<DistributedTableScanBindData>();
	if (!bind_data.pushed_query.empty()) {
		return nullptr;
	}

	auto &column_ids = get.GetColumnIds();
	for (idx_t idx = 0; idx < column_ids.size(); ++idx) {
		auto column_id = column_ids[idx].GetPrimaryIndex();
		if (column_id == COLUMN_IDENTIFIER_EMPTY || column_ids[idx].HasChildren()) {
			continue;
		}
		LogicalType type;
		scope.columns.emplace(ColumnBinding(get.table_index, idx), GetRemoteColumn(bind_data, column_id, type));
	}

	vector<string> predicates;
	for (auto &entry : get.table_filters.filters) {
		LogicalType type;
		auto column = GetRemoteColumn(bind_data, entry.first, type);
		if (!SupportsRemoteFilterPushdown(type)) {
			return nullptr;
		}
		auto predicate = RemoteFilterToSQL(*entry.second, column);
		if (!predicate.empty()) {
			predicates.emplace_back(std::move(predicate));
		}
	}

	vector<string> select_list;
	vector<string> group_by;
	vector<LogicalType> types;
	vector<string> names;
	for (auto &group : aggregate.groups) {
		auto sql = RenderExpression(scope, *group);
		if (sql.empty() || !SupportsRemoteFilterPushdown(group->return_type)) {
			return nullptr;
		}
		select_list.emplace_back(std::move(sql));
		group_by.emplace_back(std::to_string(group_by.size() + 1));
		types.emplace_back(group->return_type);
		names.emplace_back(group->GetName());
	}
	for (auto &expr : aggregate.expressions) {
		auto sql = RenderAggregate(scope, *expr);
		if (sql.empty() || !SupportsRemoteFilterPushdown(expr->return_type)) {
			return nullptr;
		}
		select_list.emplace_back(std::move(sql));
		types.emplace_back(expr->return_type);
		names.emplace_back(expr->GetName());
	}

	auto query =
	    StringUtil::Format("SELECT %s FROM %s", StringUtil::Join(select_list, ", "), bind_data.remote_table_name);
	if (!predicates.empty()) {
		query += " WHERE " + StringUtil::Join(predicates, " AND ");
	}
	if (!group_by.empty()) {
		query += " GROUP BY " + StringUtil::Join(group_by, ", ");
	}

	auto pushed_bind_data = unique_ptr_cast<FunctionData, DistributedTableScanBindData>(bind_data.Copy());
	pushed_bind_data->pushed_query = std::move(query);
	pushed_bind_data->pushed_types = types;

	const auto table_index = binder.GenerateTableIndex();
	auto result =
	    make_uniq<LogicalGet>(table_index, get.function, std::move(pushed_bind_data), types, std::move(names));
	vector<ColumnIndex> result_column_ids;
	for (idx_t idx = 0; idx < types.size(); ++idx) {
		result_column_ids.emplace_back(idx);
	}
	result->SetColumnIds(std::move(result_column_ids));

	const auto group_count = aggregate.groups.size();
	for (idx_t idx = 0; idx < group_count; ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.group_index, idx), ColumnBinding(table_index, idx));
	}
	for (idx_t idx = 0; idx < aggregate.expressions.size(); ++idx) {
		replacements.emplace_back(ColumnBinding(aggregate.aggregate_index, idx),
		                          ColumnBinding(table_index, group_count + idx));
	}
	return std::move(result);
}

void PushdownAggregates(Binder &binder, unique_ptr<LogicalOperator> &op, vector<ReplacementBinding> &replacements) {
	for (auto &child : op->children) {
		PushdownAggregates(binder, child, replacements);
	}
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto pushed = TryPushdownAggregate(binder, op->Cast<LogicalAggregate>(), replacements);
	if (pushed) {
		op = std::move(pushed);
	}
}

void OptimizeDistributedAggregates(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	vector<ReplacementBinding> replacements;
	PushdownAggregates(input.optimizer.binder, plan, replacements);
	if (replacements.empty()) {
		return;
	}
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings = std::move(replacements);
	replacer.VisitOperator(*plan);
}

} // namespace

OptimizerExtension GetDistributedAggregatePushdownExtension() {
	OptimizerExtension extension;
	extension.optimize_function = OptimizeDistributedAggregates;
	return extension;
}

} // namespace duckdb
