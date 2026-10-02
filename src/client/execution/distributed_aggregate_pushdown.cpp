#include "client/execution/distributed_aggregate_pushdown.hpp"

#include "client/execution/distributed_table_scan_function.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "utils/sql_render_utils.hpp"

namespace duckdb {

namespace {

// Returns a remote scan computing `aggregate` on the server, or nullptr if the aggregate cannot be pushed down.
unique_ptr<LogicalOperator> TryPushdownAggregate(Binder &binder, LogicalAggregate &aggregate,
                                                 vector<ReplacementBinding> &replacements) {
	auto get = GetAggregateScan(aggregate);
	if (!get || get->function.name != "distributed_scan" || get->extra_info.sample_options) {
		return nullptr;
	}
	auto &bind_data = get->bind_data->Cast<DistributedTableScanBindData>();
	if (!bind_data.pushed_query.empty()) {
		return nullptr;
	}
	auto query = RenderAggregateQuery(aggregate, *get, bind_data.table, bind_data.remote_table_name);
	if (!query) {
		return nullptr;
	}

	auto pushed_bind_data = unique_ptr_cast<FunctionData, DistributedTableScanBindData>(bind_data.Copy());
	pushed_bind_data->pushed_query = std::move(query->sql);
	pushed_bind_data->pushed_types = query->types;

	const auto table_index = binder.GenerateTableIndex();
	auto result = make_uniq<LogicalGet>(table_index, get->function, std::move(pushed_bind_data),
	                                    std::move(query->types), std::move(query->names));
	vector<ColumnIndex> result_column_ids;
	for (idx_t idx = 0; idx < result->returned_types.size(); ++idx) {
		result_column_ids.emplace_back(idx);
	}
	result->SetColumnIds(std::move(result_column_ids));
	ReplaceAggregateBindings(aggregate, table_index, replacements);
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
