#include "server/driver/worker_fragment_pushdown.hpp"

#include "arrow_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/query_utils.hpp"
#include "utils/sql_render_utils.hpp"

#include <chrono>
#include <utility>

namespace duckdb {

namespace {

struct WorkerFragmentBindData : public TableFunctionData {
	explicit WorkerFragmentBindData(WorkerFragment fragment_p) : fragment(std::move(fragment_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<WorkerFragmentBindData>(fragment);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<WorkerFragmentBindData>().fragment;
		return other.plan == fragment.plan && other.task_plans == fragment.task_plans && other.types == fragment.types;
	}

	WorkerFragment fragment;
};

struct WorkerFragmentGlobalState : public GlobalTableFunctionState {
	vector<unique_ptr<DataChunk>> chunks;
	idx_t chunk_idx = 0;
	idx_t chunk_offset = 0;
};

unique_ptr<GlobalTableFunctionState> WorkerFragmentInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<WorkerFragmentBindData>();
	auto result = make_uniq<WorkerFragmentGlobalState>();
	result->chunks = context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME)
	                     ->Execute(context, bind_data.fragment);
	return std::move(result);
}

void WorkerFragmentExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<WorkerFragmentGlobalState>();
	while (state.chunk_idx < state.chunks.size() && state.chunk_offset == state.chunks[state.chunk_idx]->size()) {
		state.chunks[state.chunk_idx++].reset();
		state.chunk_offset = 0;
	}
	if (state.chunk_idx == state.chunks.size()) {
		output.SetCardinality(0);
		return;
	}
	auto &chunk = *state.chunks[state.chunk_idx];
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, chunk.size() - state.chunk_offset);
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); ++col_idx) {
		VectorOperations::Copy(chunk.data[col_idx], output.data[col_idx], state.chunk_offset + count,
		                       state.chunk_offset, /*target_offset=*/0);
	}
	output.SetCardinality(count);
	state.chunk_offset += count;
}

// Returns the table `get` scans if workers can partition it, or nullptr.
// The executor's connection cannot see the client's temporary tables.
optional_ptr<TableCatalogEntry> GetPartitionedTable(LogicalGet &get, const WorkerFragmentState &state) {
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || table->temporary || get.extra_info.sample_options ||
	    !state.IsSameCatalog(*table)) {
		return nullptr;
	}
	return table;
}

string GetTableName(TableCatalogEntry &table) {
	return StringUtil::Format("%s.%s.%s", KeywordHelper::WriteOptionallyQuoted(table.catalog.GetName()),
	                          KeywordHelper::WriteOptionallyQuoted(table.schema.name),
	                          KeywordHelper::WriteOptionallyQuoted(table.name));
}

unique_ptr<LogicalGet> CreateFragment(Binder &binder, WorkerFragment fragment) {
	TableFunction function("worker_fragment", {}, WorkerFragmentExecute, nullptr, WorkerFragmentInitGlobal);
	auto types = fragment.types;
	vector<string> names;
	names.reserve(types.size());
	for (idx_t idx = 0; idx < types.size(); ++idx) {
		names.emplace_back(StringUtil::Format("col%llu", idx));
	}
	auto bind_data = make_uniq<WorkerFragmentBindData>(std::move(fragment));
	auto result = make_uniq<LogicalGet>(binder.GenerateTableIndex(), std::move(function), std::move(bind_data),
	                                    std::move(types), std::move(names));
	vector<ColumnIndex> column_ids;
	for (idx_t idx = 0; idx < result->returned_types.size(); ++idx) {
		column_ids.emplace_back(idx);
	}
	result->SetColumnIds(std::move(column_ids));
	return result;
}

// Collects the scans in op. Returns false if workers cannot read one, or if workers reading the same rows could
// compute different results.
bool CollectScans(LogicalOperator &op, const WorkerFragmentState &state, vector<reference<LogicalGet>> &scans) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.function.name != "seq_scan" || !GetPartitionedTable(get, state)) {
			return false;
		}
		scans.emplace_back(get);
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return false;
	default:
		break;
	}
	for (auto &child : op.children) {
		if (!CollectScans(*child, state, scans)) {
			return false;
		}
	}
	return true;
}

// Collects the scans whose rows can be split across tasks: those reached only through filters, projections and inner
// Joins. Below an aggregate or another Join type, a partial input would change the result.
void CollectPartitionableScans(LogicalOperator &op, vector<reference<LogicalGet>> &scans) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		scans.emplace_back(op.Cast<LogicalGet>());
		return;
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		if (op.Cast<LogicalComparisonJoin>().join_type != JoinType::INNER) {
			return;
		}
		break;
	default:
		return;
	}
	for (auto &child : op.children) {
		CollectPartitionableScans(*child, scans);
	}
}

optional_ptr<LogicalGet> FindScan(LogicalOperator &op, idx_t table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_GET && op.Cast<LogicalGet>().table_index == table_index) {
		return op.Cast<LogicalGet>();
	}
	for (auto &child : op.children) {
		auto result = FindScan(*child, table_index);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

// Restricts get to rowids [start, end), or from start on if end is invalid, which the worker's row group range
// scan turns into whole row groups.
void AddRowIdRange(LogicalGet &get, idx_t start, optional_idx end) {
	auto &column_ids = get.GetMutableColumnIds();
	bool has_row_id = false;
	for (const auto &column : column_ids) {
		has_row_id = has_row_id || column.IsRowIdColumn();
	}
	if (!has_row_id) {
		// Keep the scan's output columns, and so the bindings above it, unchanged.
		if (get.projection_ids.empty()) {
			for (idx_t idx = 0; idx < column_ids.size(); ++idx) {
				get.projection_ids.emplace_back(idx);
			}
		}
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	auto filter = make_uniq<ConjunctionAndFilter>();
	filter->child_filters.emplace_back(make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                             Value::BIGINT(NumericCast<int64_t>(start))));
	if (end.IsValid()) {
		filter->child_filters.emplace_back(make_uniq<ConstantFilter>(
		    ExpressionType::COMPARE_LESSTHAN, Value::BIGINT(NumericCast<int64_t>(end.GetIndex()))));
	}
	get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID), std::move(filter));
}

// Returns the input of an equality Join in op that reads scan through filters and projections, or nullptr.
optional_ptr<LogicalOperator> FindJoinInput(LogicalOperator &op, LogicalGet &scan) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		bool has_equality = false;
		for (const auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			has_equality = has_equality || condition.comparison == ExpressionType::COMPARE_EQUAL;
		}
		for (auto &child : op.children) {
			auto *input = child.get();
			while (input->type == LogicalOperatorType::LOGICAL_FILTER ||
			       input->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				input = input->children[0].get();
			}
			if (has_equality && input == &scan) {
				return child.get();
			}
		}
	}
	for (auto &child : op.children) {
		auto result = FindJoinInput(*child, scan);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

// Whether BindAverage reproduces avg over type exactly. Floating-point sums depend on the order of the rows.
bool IsExactAverageInput(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

// Finishes avg over input_type from the merged sum and count of its input as DuckDB's avg does: it divides the
// integer sum by the count times the decimal scale, in double for SMALLINT and DECIMAL(4) inputs and in long double
// otherwise.
unique_ptr<Expression> BindAverage(const LogicalType &input_type, unique_ptr<Expression> sum,
                                   unique_ptr<Expression> count) {
	const bool decimal = input_type.id() == LogicalTypeId::DECIMAL;
	const bool int64_sum = input_type.InternalType() == PhysicalType::INT16;
	const double scale = decimal ? Hugeint::Cast<double>(Hugeint::POWERS_OF_TEN[DecimalType::GetScale(input_type)]) : 1;
	ScalarFunction function("duckherder_merge_avg", {sum->return_type, count->return_type}, LogicalType::DOUBLE,
	                        [decimal, int64_sum, scale](DataChunk &args, ExpressionState &state, Vector &result) {
		                        BinaryExecutor::ExecuteWithNulls<hugeint_t, hugeint_t, double>(
		                            args.data[0], args.data[1], result, args.size(),
		                            [&](hugeint_t total, hugeint_t rows, ValidityMask &mask, idx_t idx) {
			                            const auto row_count = Hugeint::Cast<uint64_t>(rows);
			                            if (row_count == 0) {
				                            mask.SetInvalid(idx);
				                            return 0.0;
			                            }
			                            if (int64_sum) {
				                            auto divisor = static_cast<double>(row_count);
				                            if (decimal) {
					                            divisor *= scale;
				                            }
				                            return static_cast<double>(Hugeint::Cast<int64_t>(total)) / divisor;
			                            }
			                            auto divisor = static_cast<long double>(row_count);
			                            if (decimal) {
				                            divisor *= scale;
			                            }
			                            return static_cast<double>(Hugeint::Cast<long double>(total) / divisor);
		                            });
	                        });
	vector<unique_ptr<Expression>> children;
	children.emplace_back(std::move(sum));
	children.emplace_back(std::move(count));
	return make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(function), std::move(children), nullptr);
}

// Binds the aggregate name over child, or returns nullptr if it has no overload for the child's type.
unique_ptr<Expression> BindAggregate(ClientContext &context, const string &name, unique_ptr<Expression> child) {
	// The untyped lookup avoids odr-using AggregateFunctionCatalogEntry::Name, which GCC then defines twice.
	auto &entry =
	    Catalog::GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, SYSTEM_CATALOG, DEFAULT_SCHEMA, name)
	        .Cast<AggregateFunctionCatalogEntry>();
	FunctionBinder binder(context);
	ErrorData error;
	auto function_idx = binder.BindFunction(name, entry.functions, {child->return_type}, error);
	if (!function_idx.IsValid()) {
		return nullptr;
	}
	vector<unique_ptr<Expression>> children;
	children.emplace_back(std::move(child));
	return binder.BindAggregateFunction(entry.functions.GetFunctionByOffset(function_idx.GetIndex()),
	                                    std::move(children));
}

unique_ptr<Expression> ColumnRef(const LogicalType &type, idx_t table_index, idx_t column_index) {
	return make_uniq<BoundColumnRefExpression>(type, ColumnBinding(table_index, column_index));
}

string DescribeFragment(const string &head, const vector<reference<LogicalGet>> &scans, LogicalGet &partitioned) {
	vector<string> tables;
	tables.reserve(scans.size());
	for (const auto &scan : scans) {
		tables.emplace_back(GetTableName(*scan.get().GetTable()));
		if (scans.size() > 1 && &scan.get() == &partitioned) {
			tables.back() = StringUtil::Format("%s (partitioned)", tables.back());
		}
	}
	return StringUtil::Format("%s over %s", head, StringUtil::Join(tables, ", "));
}

// Replaces aggregate with a fragment whose tasks each run it on one row group range of its largest partitionable
// table, reading any other tables in full. The driver merges the tasks' partial aggregates with a second aggregate,
// and a projection above it finishes count and avg and takes over the original aggregate's bindings.
unique_ptr<LogicalOperator> TryCreateAggregateFragment(ClientContext &context, Binder &binder,
                                                       WorkerFragmentState &state, LogicalAggregate &aggregate,
                                                       vector<ReplacementBinding> &replacements) {
	if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty()) {
		return nullptr;
	}
	vector<reference<LogicalGet>> scans;
	vector<reference<LogicalGet>> candidates;
	if (!CollectScans(aggregate, state, scans)) {
		return nullptr;
	}
	CollectPartitionableScans(*aggregate.children[0], candidates);
	// Only one table or an inner Join of two tables is distributed.
	if (candidates.empty() || candidates.size() != scans.size()) {
		return nullptr;
	}
	// A Join partitions the input with the most estimated bytes, so each worker repeats the cheaper one. Use the Join's
	// input roots so filters and projections above the scans contribute to the estimate; serialization drops the
	// estimates, so this is decided here.
	idx_t partitioned_idx = 0;
	vector<JoinInputCost> costs;
	if (scans.size() > 1) {
		for (const auto &scan : scans) {
			auto input = FindJoinInput(*aggregate.children[0], scan.get());
			auto cost = input ? EstimateJoinInputCost(*input, scan.get()) : std::nullopt;
			if (!cost) {
				return nullptr;
			}
			costs.emplace_back(*cost);
			if (costs.back().scan_bytes > costs[partitioned_idx].scan_bytes) {
				partitioned_idx = costs.size() - 1;
			}
		}
	}
	auto &partitioned = scans[partitioned_idx].get();

	// Workers return the groups and partial aggregates. avg becomes a sum and a count, appended after the others.
	enum class MergeKind { SAME, COUNT, AVG };
	vector<MergeKind> kinds;
	vector<string> merge_names;
	kinds.reserve(aggregate.expressions.size());
	merge_names.reserve(aggregate.expressions.size());
	for (const auto &expr : aggregate.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return nullptr;
		}
		auto &partial = expr->Cast<BoundAggregateExpression>();
		auto name = partial.function.name;
		if (partial.IsDistinct() || partial.filter || partial.order_bys) {
			return nullptr;
		}
		// The top-N variants max(x, n) and min(x, n) return lists that the same function cannot merge.
		if (partial.children.size() != (name == "count_star" ? 0 : 1)) {
			return nullptr;
		}
		if (name == "sum" || name == "sum_no_overflow" || name == "min" || name == "max") {
			kinds.emplace_back(MergeKind::SAME);
			merge_names.emplace_back(name == "sum_no_overflow" ? "sum" : name);
		} else if (name == "count" || name == "count_star") {
			kinds.emplace_back(MergeKind::COUNT);
			merge_names.emplace_back("sum");
		} else if (name == "avg" && IsExactAverageInput(partial.children[0]->return_type)) {
			kinds.emplace_back(MergeKind::AVG);
			merge_names.emplace_back("sum");
		} else {
			return nullptr;
		}
	}
	// Maps from the index of each avg to the column of the count added for it.
	vector<idx_t> count_columns(kinds.size());
	unique_ptr<LogicalOperator> worker_plan;
	WorkerFragment fragment;
	try {
		// Copy through the serializer, as the worker reads it, and leave the client's plan unchanged.
		worker_plan = aggregate.Copy(context);
		auto &worker = worker_plan->Cast<LogicalAggregate>();
		for (idx_t idx = 0; idx < kinds.size(); ++idx) {
			if (kinds[idx] != MergeKind::AVG) {
				continue;
			}
			auto &input = worker.expressions[idx]->Cast<BoundAggregateExpression>().children[0];
			auto sum = BindAggregate(context, "sum", input->Copy());
			auto count = BindAggregate(context, "count", input->Copy());
			if (!sum || !count) {
				return nullptr;
			}
			worker.expressions[idx] = std::move(sum);
			worker.expressions.emplace_back(std::move(count));
			count_columns[idx] = worker.expressions.size() - 1;
			merge_names.emplace_back("sum");
		}
		// A projection keeps the columns the driver expects if the worker's optimizer merges duplicate aggregates, such
		// as a sum added for avg.
		worker.ResolveOperatorTypes();
		auto bindings = worker.GetColumnBindings();
		vector<unique_ptr<Expression>> columns;
		columns.reserve(bindings.size());
		for (idx_t idx = 0; idx < bindings.size(); ++idx) {
			columns.emplace_back(ColumnRef(worker.types[idx], bindings[idx].table_index, bindings[idx].column_index));
		}
		auto root = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(columns));
		root->children.emplace_back(std::move(worker_plan));
		worker_plan = std::move(root);
		worker_plan->ResolveOperatorTypes();
		fragment.plan = PlanSerializer::SerializeLogicalPlan(*worker_plan);
		fragment.task_plans = SerializeTaskPlans(context, *worker_plan,
		                                         *FindScan(*worker_plan, partitioned.table_index), state.NumWorkers());
	} catch (const std::exception &) {
		// Some operators and table filters cannot be serialized.
		return nullptr;
	}
	// Repeating the other tables' scans in every task only pays off if a Join is split across workers.
	if (scans.size() > 1) {
		const auto num_tasks = fragment.task_plans.size();
		if (num_tasks < 2) {
			return nullptr;
		}
		// Bound the replicated hash build estimate and charge for every additional scan/build. Only distribute when
		// the parallel scan savings exceed this duplicated work.
		constexpr double MAX_REPLICATED_BUILD_BYTES = 64 * 1024 * 1024;
		const auto &partition_cost = costs[partitioned_idx];
		const auto &replicated_cost = costs[1 - partitioned_idx];
		const auto extra_replication =
		    static_cast<double>(num_tasks - 1) * (replicated_cost.scan_bytes + replicated_cost.build_bytes);
		const auto parallel_savings = partition_cost.scan_bytes * (1.0 - 1.0 / static_cast<double>(num_tasks));
		if (replicated_cost.build_bytes > MAX_REPLICATED_BUILD_BYTES || extra_replication >= parallel_savings) {
			return nullptr;
		}
	}

	const auto group_count = aggregate.groups.size();
	const auto partial_types = worker_plan->types;
	vector<string> exprs;
	vector<string> groups;
	exprs.reserve(aggregate.expressions.size());
	groups.reserve(group_count);
	for (const auto &expr : aggregate.expressions) {
		exprs.emplace_back(expr->ToString());
	}
	for (const auto &group : aggregate.groups) {
		groups.emplace_back(group->ToString());
	}
	auto head = StringUtil::Format("aggregate %s", StringUtil::Join(exprs, ", "));
	if (!groups.empty()) {
		head = StringUtil::Format("%s group by %s", head, StringUtil::Join(groups, ", "));
	}
	fragment.types = partial_types;
	fragment.description = DescribeFragment(head, scans, partitioned);
	fragment.merge_strategy = groups.empty() ? QueryPlanAnalyzer::MergeStrategy::AGGREGATE_MERGE
	                                         : QueryPlanAnalyzer::MergeStrategy::GROUP_BY_MERGE;
	auto get = CreateFragment(binder, std::move(fragment));

	vector<unique_ptr<Expression>> merges;
	merges.reserve(merge_names.size());
	for (idx_t idx = 0; idx < merge_names.size(); ++idx) {
		const auto column = group_count + idx;
		auto merge =
		    BindAggregate(context, merge_names[idx], ColumnRef(partial_types[column], get->table_index, column));
		// A partial aggregate merged with the same function must keep its type, e.g. sum over DECIMAL(38, 2).
		if (!merge || (idx < kinds.size() && kinds[idx] == MergeKind::SAME &&
		               merge->return_type != aggregate.expressions[idx]->return_type)) {
			return nullptr;
		}
		merges.emplace_back(std::move(merge));
	}
	auto merge =
	    make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), binder.GenerateTableIndex(), std::move(merges));
	vector<unique_ptr<Expression>> outputs;
	outputs.reserve(group_count + kinds.size());
	for (idx_t idx = 0; idx < group_count; ++idx) {
		merge->groups.emplace_back(ColumnRef(partial_types[idx], get->table_index, idx));
		outputs.emplace_back(ColumnRef(partial_types[idx], merge->group_index, idx));
	}
	merge->grouping_sets = aggregate.grouping_sets;
	for (idx_t idx = 0; idx < kinds.size(); ++idx) {
		auto &merged = *merge->expressions[idx];
		auto output = ColumnRef(merged.return_type, merge->aggregate_index, idx);
		if (kinds[idx] == MergeKind::COUNT) {
			// Every task returns a count, also for empty input, so the merged sum is never NULL.
			output = BoundCastExpression::AddCastToType(context, std::move(output), LogicalType::BIGINT);
		} else if (kinds[idx] == MergeKind::AVG) {
			auto &count = *merge->expressions[count_columns[idx]];
			// BindAverage reads both merged sums as hugeints.
			if (merged.return_type.InternalType() != PhysicalType::INT128 ||
			    count.return_type.InternalType() != PhysicalType::INT128) {
				return nullptr;
			}
			auto &input_type = aggregate.expressions[idx]->Cast<BoundAggregateExpression>().children[0]->return_type;
			output = BindAverage(input_type, std::move(output),
			                     ColumnRef(count.return_type, merge->aggregate_index, count_columns[idx]));
		}
		outputs.emplace_back(std::move(output));
	}
	merge->children.emplace_back(std::move(get));
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(outputs));
	projection->children.emplace_back(std::move(merge));
	ReplaceAggregateBindings(aggregate, projection->table_index, replacements);
	return std::move(projection);
}

unique_ptr<LogicalOperator> TryCreateScanFragment(ClientContext &context, Binder &binder, WorkerFragmentState &state,
                                                  LogicalGet &get, vector<ReplacementBinding> &replacements) {
	auto table = GetPartitionedTable(get, state);
	auto &column_ids = get.GetColumnIds();
	if (!table || column_ids.empty()) {
		return nullptr;
	}
	for (const auto &column : column_ids) {
		if (column.HasChildren() || column.GetPrimaryIndex() == COLUMN_IDENTIFIER_EMPTY) {
			return nullptr;
		}
	}
	WorkerFragment fragment;
	try {
		fragment.plan = PlanSerializer::SerializeLogicalPlan(get);
		fragment.task_plans = SerializeTaskPlans(context, get, get, state.NumWorkers());
	} catch (const std::exception &) {
		return nullptr;
	}
	get.ResolveOperatorTypes();
	fragment.types = get.types;
	fragment.description = StringUtil::Format("scan over %s", GetTableName(*table));
	auto bindings = get.GetColumnBindings();
	auto result = CreateFragment(binder, std::move(fragment));
	for (idx_t idx = 0; idx < bindings.size(); ++idx) {
		replacements.emplace_back(bindings[idx], ColumnBinding(result->table_index, idx));
	}
	return std::move(result);
}

idx_t CountScans(LogicalOperator &op) {
	idx_t count = op.type == LogicalOperatorType::LOGICAL_GET ? 1 : 0;
	for (auto &child : op.children) {
		count += CountScans(*child);
	}
	return count;
}

void OptimizeWorkerFragments(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// Fragments run outside the client's transaction, so they would miss its snapshot in an explicit transaction.
	auto state = input.context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME);
	if (!state || !input.context.transaction.IsAutoCommit() || state->NumWorkers() == 0) {
		return;
	}
	auto &binder = input.optimizer.binder;
	// A Join of two scans only becomes a fragment at its aggregate; everything above it stays in the plan.
	const auto scans = CountScans(*plan);
	if (scans != 1 && scans != 2) {
		return;
	}
	vector<ReplacementBinding> replacements;
	reference<unique_ptr<LogicalOperator>> node = plan;
	while (true) {
		auto &op = *node.get();
		unique_ptr<LogicalOperator> fragment;
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			fragment =
			    TryCreateAggregateFragment(input.context, binder, *state, op.Cast<LogicalAggregate>(), replacements);
		} else if (op.type == LogicalOperatorType::LOGICAL_GET) {
			fragment = TryCreateScanFragment(input.context, binder, *state, op.Cast<LogicalGet>(), replacements);
		}
		if (fragment) {
			node.get() = std::move(fragment);
			break;
		}
		// Below a limit only a few rows may be needed, which the driver reads faster than workers transfer the table.
		if (op.type == LogicalOperatorType::LOGICAL_LIMIT || op.children.size() != 1) {
			return;
		}
		node = op.children[0];
	}
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings = std::move(replacements);
	replacer.VisitOperator(*plan);
}

} // namespace

vector<string> SerializeTaskPlans(ClientContext &context, LogicalOperator &plan, LogicalGet &partitioned,
                                  idx_t max_tasks) {
	vector<idx_t> row_group_starts;
	auto row_groups = partitioned.GetTable()->GetStorage().GetRowGroupCollection()->GetRowGroups();
	for (auto segment = row_groups->GetRootSegment(); segment; segment = row_groups->GetNextSegment(*segment)) {
		row_group_starts.emplace_back(segment->GetRowStart());
	}
	// Rowid pruning is row-group granular, so splitting a row group would make each task read all of it.
	const auto num_tasks = MinValue<idx_t>(max_tasks, row_group_starts.size());
	if (num_tasks <= 1) {
		return {PlanSerializer::SerializeLogicalPlan(plan)};
	}
	// The row groups are those at planning time. The first task starts at rowid 0 and the last has no upper bound, so
	// the tasks still cover every row if rows are appended before the plan executes, e.g. by a prepared statement.
	vector<string> plans;
	plans.reserve(num_tasks);
	for (idx_t task = 0; task < num_tasks; ++task) {
		const auto first = task * row_group_starts.size() / num_tasks;
		const auto last = (task + 1) * row_group_starts.size() / num_tasks;
		auto copy = plan.Copy(context);
		AddRowIdRange(*FindScan(*copy, partitioned.table_index), task == 0 ? 0 : row_group_starts[first],
		              task + 1 == num_tasks ? optional_idx() : optional_idx(row_group_starts[last]));
		plans.emplace_back(PlanSerializer::SerializeLogicalPlan(*copy));
	}
	return plans;
}

WorkerFragmentState::WorkerFragmentState(DistributedExecutor &executor_p, Connection &connection_p)
    : executor(executor_p), connection(connection_p),
      catalog(DatabaseManager::Get(*connection_p.context)
                  .GetDatabase(DatabaseManager::GetDefaultDatabase(*connection_p.context))
                  ->GetCatalog()) {
}

vector<unique_ptr<DataChunk>> WorkerFragmentState::Execute(ClientContext &context, const WorkerFragment &fragment) {
	QueryExecutionInfo info;
	info.sql = fragment.description;
	const auto start = std::chrono::steady_clock::now();
	vector<arrow::RecordBatchVector> task_batches;
	vector<unique_ptr<DataChunk>> chunks;
	if (executor.ExecutePlans(fragment.task_plans, task_batches)) {
		for (auto &batches : task_batches) {
			for (auto &batch : batches) {
				auto chunk = make_uniq<DataChunk>();
				ArrowRecordBatchToDataChunk(context, *batch, *chunk, &fragment.types);
				batch.reset();
				chunks.emplace_back(std::move(chunk));
			}
		}
		info.execution_mode =
		    fragment.task_plans.size() == 1 ? QueryExecutionMode::DELEGATED : QueryExecutionMode::ROW_GROUP_PARTITION;
		info.merge_strategy = fragment.merge_strategy;
		info.num_workers_used = fragment.task_plans.size();
		info.num_tasks_generated = fragment.task_plans.size();
	} else {
		auto plan = PlanSerializer::DeserializeLogicalPlan(*connection.context, fragment.plan);
		auto result = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
		if (result->HasError()) {
			result->ThrowError();
		}
		if (result->types != fragment.types) {
			throw InternalException("Worker fragment '%s' returned unexpected types", fragment.description);
		}
		for (auto chunk = result->Fetch(); chunk && chunk->size() > 0; chunk = result->Fetch()) {
			chunks.emplace_back(std::move(chunk));
		}
	}
	info.query_duration =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
	executions.emplace_back(std::move(info));
	return chunks;
}

bool WorkerFragmentState::IsSameCatalog(const TableCatalogEntry &table) const {
	return &table.catalog == &catalog;
}

vector<QueryExecutionInfo> WorkerFragmentState::TakeExecutions() {
	return std::exchange(executions, {});
}

idx_t WorkerFragmentState::NumWorkers() {
	return executor.NumWorkers();
}

void RegisterWorkerFragments(DBConfig &config) {
	// Compressed materialization bakes the driver's statistics into the plan, and workers may read newer data.
	config.options.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
	OptimizerExtension extension;
	extension.optimize_function = OptimizeWorkerFragments;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace duckdb
