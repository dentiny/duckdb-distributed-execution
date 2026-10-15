#include "server/driver/worker_fragment_pushdown.hpp"

#include "arrow_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/error_data.hpp"
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
#include "duckdb/parallel/async_result.hpp"
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
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/key_range_partition.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/query_utils.hpp"
#include "utils/sql_render_utils.hpp"

#include <algorithm>
#include <arrow/flight/client.h>
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

// The fragment's result: the stream of each worker task, each read by one thread at a time, or the chunks of its local
// execution.
class WorkerFragmentGlobalState : public GlobalTableFunctionState {
public:
	WorkerFragmentGlobalState(shared_ptr<WorkerFragmentState> fragment_state_p, idx_t max_threads_p)
	    : fragment_state(std::move(fragment_state_p)), max_threads(max_threads_p) {
	}

	~WorkerFragmentGlobalState() override {
		// Stops the workers of a query that ends before reading all their rows, e.g. on an error.
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		if (finished_streams < streams.size()) {
			for (auto &stream : streams) {
				stream->Cancel();
			}
		}
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

	// Starts the fragment on the first call; later calls wait until it has started.
	void Start(const WorkerFragment &fragment) {
		const concurrency::lock_guard<concurrency::mutex> start_lock(start_mu);
		if (started) {
			return;
		}
		started = true;
		start_time = std::chrono::steady_clock::now();
		info.sql = fragment.description;
		vector<std::unique_ptr<arrow::flight::FlightStreamReader>> task_streams;
		if (fragment_state->StartTasks(fragment, task_streams)) {
			info.execution_mode =
			    task_streams.size() == 1 ? QueryExecutionMode::DELEGATED : QueryExecutionMode::ROW_GROUP_PARTITION;
			info.merge_strategy = fragment.merge_strategy;
			info.num_workers_used = task_streams.size();
			info.num_tasks_generated = task_streams.size();
			const concurrency::lock_guard<concurrency::mutex> lock(mu);
			streams = std::move(task_streams);
			return;
		}
		auto local_chunks = fragment_state->ExecuteLocally(fragment);
		RecordExecution();
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		chunks = std::move(local_chunks);
	}

	// Returns the next chunk for a thread reading the stream at stream_idx, if any, or nullptr if there are no rows
	// left. Reads the next unclaimed stream once that stream ends.
	unique_ptr<DataChunk> Next(ClientContext &context, const WorkerFragment &fragment, optional_idx &stream_idx) {
		while (true) {
			optional_ptr<arrow::flight::FlightStreamReader> stream;
			{
				const concurrency::lock_guard<concurrency::mutex> lock(mu);
				if (streams.empty()) {
					return next_chunk < chunks.size() ? std::move(chunks[next_chunk++]) : nullptr;
				}
				if (!stream_idx.IsValid()) {
					if (next_stream == streams.size()) {
						return nullptr;
					}
					stream_idx = next_stream++;
				}
				stream = streams[stream_idx.GetIndex()].get();
			}
			auto next = stream->Next();
			if (!next.ok()) {
				throw IOException("Worker task %llu of fragment '%s' failed: %s", stream_idx.GetIndex(),
				                  fragment.description, next.status().ToString());
			}
			if (next->data) {
				if (next->data->num_rows() == 0) {
					continue;
				}
				auto chunk = make_uniq<DataChunk>();
				ArrowRecordBatchToDataChunk(context, *next->data, *chunk, &fragment.types);
				return chunk;
			}
			stream_idx = optional_idx();
			bool all_finished;
			{
				const concurrency::lock_guard<concurrency::mutex> lock(mu);
				all_finished = ++finished_streams == streams.size();
			}
			if (all_finished) {
				RecordExecution();
			}
		}
	}

	void SetError(ErrorData error_p) {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		if (!error.HasError()) {
			error = std::move(error_p);
		}
	}

	void ThrowIfError() {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		if (error.HasError()) {
			error.Throw();
		}
	}

private:
	void RecordExecution() {
		info.query_duration =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
		fragment_state->RecordExecution(std::move(info));
	}

	const shared_ptr<WorkerFragmentState> fragment_state;
	const idx_t max_threads;
	concurrency::mutex start_mu;
	bool started DUCKDB_GUARDED_BY(start_mu) = false;
	// Written by Start, and read once the fragment has ended.
	std::chrono::steady_clock::time_point start_time;
	QueryExecutionInfo info;
	concurrency::mutex mu;
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> streams DUCKDB_GUARDED_BY(mu);
	idx_t next_stream DUCKDB_GUARDED_BY(mu) = 0;
	idx_t finished_streams DUCKDB_GUARDED_BY(mu) = 0;
	vector<unique_ptr<DataChunk>> chunks DUCKDB_GUARDED_BY(mu);
	idx_t next_chunk DUCKDB_GUARDED_BY(mu) = 0;
	ErrorData error DUCKDB_GUARDED_BY(mu);
};

struct WorkerFragmentLocalState : public LocalTableFunctionState {
	bool HasRows() const {
		return chunk && offset < chunk->size();
	}

	optional_idx stream_idx;
	unique_ptr<DataChunk> chunk;
	idx_t offset = 0;
	bool finished = false;
};

// Reads the next chunk on a scheduler thread, so the source blocks instead of the executor polling while workers
// compute and send it.
class WorkerFragmentReadTask : public AsyncTask {
public:
	WorkerFragmentReadTask(ClientContext &context_p, const WorkerFragment &fragment_p,
	                       WorkerFragmentGlobalState &global_p, WorkerFragmentLocalState &local_p)
	    : context(context_p), fragment(fragment_p), global(global_p), local(local_p) {
	}

	void Execute() override {
		try {
			global.Start(fragment);
			local.chunk = global.Next(context, fragment, local.stream_idx);
			local.offset = 0;
			local.finished = !local.chunk;
		} catch (const std::exception &ex) {
			global.SetError(ErrorData(ex));
			local.finished = true;
		}
	}

private:
	ClientContext &context;
	const WorkerFragment &fragment;
	WorkerFragmentGlobalState &global;
	WorkerFragmentLocalState &local;
};

unique_ptr<GlobalTableFunctionState> WorkerFragmentInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto fragment_state = context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME);
	if (!fragment_state) {
		throw InternalException("Worker fragment runs without a worker fragment state");
	}
	const auto &fragment = input.bind_data->Cast<WorkerFragmentBindData>().fragment;
	return make_uniq<WorkerFragmentGlobalState>(std::move(fragment_state),
	                                            MaxValue<idx_t>(fragment.task_plans.size(), 1));
}

unique_ptr<LocalTableFunctionState> WorkerFragmentInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state) {
	return make_uniq<WorkerFragmentLocalState>();
}

void WorkerFragmentExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global = data.global_state->Cast<WorkerFragmentGlobalState>();
	auto &local = data.local_state->Cast<WorkerFragmentLocalState>();
	if (!local.HasRows() && !local.finished) {
		auto task = make_uniq<WorkerFragmentReadTask>(context, data.bind_data->Cast<WorkerFragmentBindData>().fragment,
		                                              global, local);
		if (data.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
			vector<unique_ptr<AsyncTask>> tasks;
			tasks.emplace_back(std::move(task));
			data.async_result = AsyncResult(std::move(tasks));
			output.SetCardinality(0);
			return;
		}
		task->Execute();
	}
	global.ThrowIfError();
	if (!local.HasRows()) {
		output.SetCardinality(0);
		return;
	}
	auto &chunk = *local.chunk;
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, chunk.size() - local.offset);
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); ++col_idx) {
		output.data[col_idx].Slice(chunk.data[col_idx], local.offset, local.offset + count);
	}
	output.SetCardinality(count);
	local.offset += count;
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
	TableFunction function("worker_fragment", {}, WorkerFragmentExecute, nullptr, WorkerFragmentInitGlobal,
	                       WorkerFragmentInitLocal);
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
bool CollectScans(LogicalOperator &op, const WorkerFragmentState &state, vector<reference<LogicalGet>> &scans,
                  bool in_delim_join = false) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.function.name != "seq_scan" || !GetPartitionedTable(get, state)) {
			return false;
		}
		scans.emplace_back(get);
		break;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		in_delim_join = true;
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		// It reads the input of its DELIM_JOIN, which workers only have if that Join is in the fragment too. Here only
		// some DELIM_JOIN above it is checked; AnalyzeSplit rejects one whose DELIM_JOIN is not in the fragment.
		if (!in_delim_join) {
			return false;
		}
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	// It reads a result computed elsewhere in the client's plan, which workers do not have.
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return false;
	default:
		break;
	}
	for (auto &child : op.children) {
		if (!CollectScans(*child, state, scans, in_delim_join)) {
			return false;
		}
	}
	return true;
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

// Returns the input of a Join in op that reads scan through filters and projections, or nullptr, and sets equi_join to
// whether that Join compares keys.
optional_ptr<LogicalOperator> FindJoinInput(LogicalOperator &op, LogicalGet &scan, bool &equi_join) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		for (auto &child : op.children) {
			auto *input = child.get();
			while (input->type == LogicalOperatorType::LOGICAL_FILTER ||
			       input->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				input = input->children[0].get();
			}
			if (input != &scan) {
				continue;
			}
			equi_join = false;
			if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			    op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
				for (const auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
					equi_join = equi_join || IsKeyComparison(condition.comparison);
				}
			}
			return child.get();
		}
		break;
	default:
		break;
	}
	for (auto &child : op.children) {
		auto result = FindJoinInput(*child, scan, equi_join);
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

string DescribeFragment(const string &head, const vector<reference<LogicalGet>> &scans, const vector<bool> &split) {
	vector<string> tables;
	tables.reserve(scans.size());
	for (idx_t idx = 0; idx < scans.size(); ++idx) {
		tables.emplace_back(GetTableName(*scans[idx].get().GetTable()));
		if (scans.size() > 1 && split[idx]) {
			tables.back() = StringUtil::Format("%s (partitioned)", tables.back());
		}
	}
	return StringUtil::Format("%s over %s", head, StringUtil::Join(tables, ", "));
}

// Wraps plan in a projection of its columns, so workers return them in this order even if their optimizer changes the
// plan, e.g. by merging duplicate aggregates or swapping a Join's inputs.
unique_ptr<LogicalOperator> ProjectColumns(Binder &binder, unique_ptr<LogicalOperator> plan) {
	plan->ResolveOperatorTypes();
	auto bindings = plan->GetColumnBindings();
	vector<unique_ptr<Expression>> columns;
	columns.reserve(bindings.size());
	for (idx_t idx = 0; idx < bindings.size(); ++idx) {
		columns.emplace_back(ColumnRef(plan->types[idx], bindings[idx].table_index, bindings[idx].column_index));
	}
	auto root = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(columns));
	root->children.emplace_back(std::move(plan));
	root->ResolveOperatorTypes();
	return std::move(root);
}

// Workers optimize the plan again, and DuckDB's Join order optimizer expects each DELIM_JOIN to read its
// duplicate-eliminated input first, as before the build side optimizer flips it.
void UnflipDelimJoins(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN && op.Cast<LogicalComparisonJoin>().delim_flipped) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		std::swap(join.children[0], join.children[1]);
		join.join_type = InverseJoinType(join.join_type);
		for (auto &condition : join.conditions) {
			std::swap(condition.left, condition.right);
			condition.comparison = FlipComparisonExpression(condition.comparison);
		}
		std::swap(join.left_projection_map, join.right_projection_map);
		join.delim_flipped = false;
	}
	for (auto &child : op.children) {
		UnflipDelimJoins(*child);
	}
}

// Writes fragment's plan and task plans from worker_plan, the workers' copy of a plan over input, and marks in split
// which of scans the tasks split. Tries the scans from the most read bytes down: by key ranges with the inputs
// equi-joined to it on a key they are ordered on, else alone by rowid ranges; the others are replicated. Keeps the
// first split AnalyzeSplit accepts. Returns false if there is none, or if a Join should stay local.
bool PlanTasks(ClientContext &context, WorkerFragmentState &state, LogicalOperator &input,
               const vector<reference<LogicalGet>> &scans, LogicalOperator &worker_plan, WorkerFragment &fragment,
               vector<bool> &split) {
	Value setting;
	context.TryGetCurrentSetting(JOIN_PARTITIONING_SETTING, setting);
	const auto join_partitioning = setting.IsNull() ? string("auto") : StringUtil::Lower(setting.ToString());
	if (scans.size() > 1 && join_partitioning == "never") {
		return false;
	}
	// Estimates are lost in serialization, so this is decided here, at the Join's inputs. The cost model assumes hash
	// Joins, so a scan another Join reads stays local; a scan no Join reads directly, e.g. below an aggregate, is
	// charged its own output.
	vector<JoinInputCost> costs;
	if (scans.size() > 1) {
		for (const auto &scan : scans) {
			bool equi_join = false;
			auto join_input = FindJoinInput(input, scan.get(), equi_join);
			if (join_input && !equi_join) {
				return false;
			}
			auto cost = EstimateJoinInputCost(join_input ? *join_input : scan.get(), scan.get());
			if (!cost) {
				return false;
			}
			costs.emplace_back(*cost);
		}
	}
	// Scan indexes, most read bytes first. A lone scan has no costs and is not compared.
	vector<idx_t> order;
	order.reserve(scans.size());
	for (idx_t idx = 0; idx < scans.size(); ++idx) {
		order.emplace_back(idx);
	}
	std::stable_sort(order.begin(), order.end(),
	                 [&](idx_t a, idx_t b) { return costs[a].scan_bytes > costs[b].scan_bytes; });
	optional_idx partitioned_idx;
	KeyRangePartition key_ranges;
	for (const auto idx : order) {
		auto &candidate = scans[idx].get();
		if (scans.size() > 1 && FindKeyRangePartition(input, candidate, scans, costs, state.NumWorkers(), key_ranges) &&
		    IsSplit(AnalyzeSplit(input, key_ranges, optional_idx()))) {
			partitioned_idx = idx;
			break;
		}
		key_ranges = KeyRangePartition();
		if (IsSplit(AnalyzeSplit(input, key_ranges, candidate.table_index))) {
			partitioned_idx = idx;
			break;
		}
	}
	if (!partitioned_idx.IsValid()) {
		return false;
	}
	auto &partitioned = scans[partitioned_idx.GetIndex()].get();
	UnflipDelimJoins(worker_plan);
	fragment.plan = PlanSerializer::SerializeLogicalPlan(worker_plan);
	if (!key_ranges.scans.empty()) {
		fragment.task_plans = SerializeKeyRangeTaskPlans(context, worker_plan, key_ranges);
	} else {
		fragment.task_plans = SerializeTaskPlans(context, worker_plan, *FindScan(worker_plan, partitioned.table_index),
		                                         state.NumWorkers());
	}
	split.assign(scans.size(), false);
	for (idx_t idx = 0; idx < scans.size(); ++idx) {
		split[idx] = &scans[idx].get() == &partitioned;
		for (const auto &scan : key_ranges.scans) {
			split[idx] = split[idx] || scan.table_index == scans[idx].get().table_index;
		}
	}
	// Repeating the other tables' scans in every task only pays off if a Join is split across workers.
	if (scans.size() > 1) {
		const auto num_tasks = fragment.task_plans.size();
		if (num_tasks < 2) {
			return false;
		}
		// Split inputs count as partitioned; the others are replicated. A replicated input joins the stream of the
		// largest split input, so only that input's build is compared with it.
		JoinInputCost partitioned_cost {0, costs[partitioned_idx.GetIndex()].build_bytes};
		vector<JoinInputCost> replicated_costs;
		for (idx_t idx = 0; idx < costs.size(); ++idx) {
			if (split[idx]) {
				partitioned_cost.scan_bytes += costs[idx].scan_bytes;
			} else {
				replicated_costs.push_back(costs[idx]);
			}
		}
		// Workers do not report their memory, so assume each has the driver's.
		if (join_partitioning == "auto" &&
		    !ShouldPartitionJoin(partitioned_cost, replicated_costs, num_tasks,
		                         static_cast<double>(BufferManager::GetBufferManager(context).GetMaxMemory()))) {
			return false;
		}
	}
	return true;
}

// Describes aggregate over scans, marking the split ones.
string DescribeAggregate(const LogicalAggregate &aggregate, const vector<reference<LogicalGet>> &scans,
                         const vector<bool> &split) {
	vector<string> exprs;
	vector<string> groups;
	exprs.reserve(aggregate.expressions.size());
	groups.reserve(aggregate.groups.size());
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
	return DescribeFragment(head, scans, split);
}

// Whether every aggregate is DISTINCT over the same argument, like count(DISTINCT x) and sum(DISTINCT x). Duplicate
// values of x do not change their results, so tasks can return each value once.
bool IsDistinctOnly(const LogicalAggregate &aggregate) {
	for (const auto &expr : aggregate.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		// Tasks return only the argument's values, so an aggregate with a FILTER or ORDER BY, which reads other
		// columns, or with another argument cannot use them. Except for count, the result over inexact values, e.g. a
		// sum of doubles, depends on the order in which the driver receives them.
		if (!bound.IsDistinct() || bound.filter || bound.order_bys || bound.children.size() != 1 ||
		    !bound.children[0]->Equals(*aggregate.expressions[0]->Cast<BoundAggregateExpression>().children[0]) ||
		    (bound.function.name != "count" && !IsExactAverageInput(bound.children[0]->return_type))) {
			return false;
		}
	}
	return !aggregate.expressions.empty();
}

// Replaces aggregate, which IsDistinctOnly accepts, with a fragment whose tasks return the distinct groups and argument
// values of their part of the input. The driver runs the original aggregates over the union of those sets.
unique_ptr<LogicalOperator> TryCreateDistinctFragment(ClientContext &context, Binder &binder,
                                                      WorkerFragmentState &state, LogicalAggregate &aggregate,
                                                      const vector<reference<LogicalGet>> &scans) {
	const auto group_count = aggregate.groups.size();
	auto &argument = *aggregate.expressions[0]->Cast<BoundAggregateExpression>().children[0];
	WorkerFragment fragment;
	vector<bool> split;
	try {
		auto worker_plan = aggregate.Copy(context);
		auto &worker = worker_plan->Cast<LogicalAggregate>();
		worker.groups.emplace_back(argument.Copy());
		worker.expressions.clear();
		for (auto &grouping_set : worker.grouping_sets) {
			grouping_set.insert(group_count);
		}
		worker_plan = ProjectColumns(binder, std::move(worker_plan));
		if (!PlanTasks(context, state, *aggregate.children[0], scans, *worker_plan, fragment, split)) {
			return nullptr;
		}
		fragment.types = worker_plan->types;
	} catch (const std::exception &) {
		// Some operators and table filters cannot be serialized.
		return nullptr;
	}
	fragment.description = DescribeAggregate(aggregate, scans, split);
	fragment.merge_strategy = group_count == 0 ? MergeStrategy::AGGREGATE_MERGE : MergeStrategy::GROUP_BY_MERGE;
	const auto types = fragment.types;
	auto get = CreateFragment(binder, std::move(fragment));
	// The merge keeps the original indexes, so the operators above read it as they read the original aggregate.
	vector<unique_ptr<Expression>> merges;
	merges.reserve(aggregate.expressions.size());
	for (const auto &expr : aggregate.expressions) {
		auto merge = expr->Copy();
		merge->Cast<BoundAggregateExpression>().children[0] =
		    ColumnRef(types[group_count], get->table_index, group_count);
		merges.emplace_back(std::move(merge));
	}
	auto merge = make_uniq<LogicalAggregate>(aggregate.group_index, aggregate.aggregate_index, std::move(merges));
	for (idx_t idx = 0; idx < group_count; ++idx) {
		merge->groups.emplace_back(ColumnRef(types[idx], get->table_index, idx));
	}
	merge->grouping_sets = aggregate.grouping_sets;
	merge->distinct_validity = aggregate.distinct_validity;
	merge->children.emplace_back(std::move(get));
	return std::move(merge);
}

// Replaces aggregate with a fragment whose tasks each aggregate their part of the input, split as PlanTasks decides.
// The driver merges the partial aggregates with a second aggregate, and a projection above it finishes count and avg
// and takes over the original aggregate's bindings.
unique_ptr<LogicalOperator> TryCreateAggregateFragment(ClientContext &context, Binder &binder,
                                                       WorkerFragmentState &state, LogicalAggregate &aggregate,
                                                       vector<ReplacementBinding> &replacements) {
	if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty()) {
		return nullptr;
	}
	vector<reference<LogicalGet>> scans;
	if (!CollectScans(aggregate, state, scans) || scans.empty()) {
		return nullptr;
	}
	if (IsDistinctOnly(aggregate)) {
		return TryCreateDistinctFragment(context, binder, state, aggregate, scans);
	}
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
	vector<bool> split;
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
		worker_plan = ProjectColumns(binder, std::move(worker_plan));
		if (!PlanTasks(context, state, *aggregate.children[0], scans, *worker_plan, fragment, split)) {
			return nullptr;
		}
	} catch (const std::exception &) {
		// Some operators and table filters cannot be serialized.
		return nullptr;
	}
	const auto group_count = aggregate.groups.size();
	const auto partial_types = worker_plan->types;
	fragment.types = partial_types;
	fragment.description = DescribeAggregate(aggregate, scans, split);
	fragment.merge_strategy = group_count == 0 ? MergeStrategy::AGGREGATE_MERGE : MergeStrategy::GROUP_BY_MERGE;
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

// Replaces join with a fragment whose tasks each return the Join's rows for their part of the input, split as
// PlanTasks decides. The driver concatenates the rows.
unique_ptr<LogicalOperator> TryCreateJoinFragment(ClientContext &context, Binder &binder, WorkerFragmentState &state,
                                                  LogicalComparisonJoin &join,
                                                  vector<ReplacementBinding> &replacements) {
	vector<reference<LogicalGet>> scans;
	if (!CollectScans(join, state, scans) || scans.empty()) {
		return nullptr;
	}
	WorkerFragment fragment;
	vector<bool> split;
	try {
		auto worker_plan = ProjectColumns(binder, join.Copy(context));
		if (!PlanTasks(context, state, join, scans, *worker_plan, fragment, split)) {
			return nullptr;
		}
		fragment.types = worker_plan->types;
	} catch (const std::exception &) {
		return nullptr;
	}
	fragment.description = DescribeFragment("join", scans, split);
	auto bindings = join.GetColumnBindings();
	auto result = CreateFragment(binder, std::move(fragment));
	for (idx_t idx = 0; idx < bindings.size(); ++idx) {
		replacements.emplace_back(bindings[idx], ColumnBinding(result->table_index, idx));
	}
	return std::move(result);
}

// Replaces the first aggregate, Join or lone scan below root, following operators with one child, with a worker
// fragment. reduced is whether an operator above root reduces its rows.
void CutFragment(ClientContext &context, Binder &binder, WorkerFragmentState &state, unique_ptr<LogicalOperator> &root,
                 bool reduced, vector<ReplacementBinding> &replacements) {
	reference<unique_ptr<LogicalOperator>> node = root;
	while (true) {
		auto &op = *node.get();
		unique_ptr<LogicalOperator> fragment;
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			fragment = TryCreateAggregateFragment(context, binder, state, op.Cast<LogicalAggregate>(), replacements);
		} else if ((op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		            op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
		           !reduced) {
			fragment = TryCreateJoinFragment(context, binder, state, op.Cast<LogicalComparisonJoin>(), replacements);
		} else if (op.type == LogicalOperatorType::LOGICAL_GET) {
			fragment = TryCreateScanFragment(context, binder, state, op.Cast<LogicalGet>(), replacements);
		}
		if (fragment) {
			node.get() = std::move(fragment);
			return;
		}
		// A materialized CTE runs its definition once and the query reads the result, so each can hold a fragment.
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			for (auto &child : op.children) {
				CutFragment(context, binder, state, child, reduced, replacements);
			}
			return;
		}
		// Below a limit only a few rows may be needed, which the driver reads faster than workers transfer the table.
		if (op.type == LogicalOperatorType::LOGICAL_LIMIT || op.children.size() != 1) {
			return;
		}
		// A Join fragment sends all its rows to the driver, a transfer the cost model ignores, so it is only cut when
		// the operators above only project or sort them.
		reduced = reduced || (op.type != LogicalOperatorType::LOGICAL_PROJECTION &&
		                      op.type != LogicalOperatorType::LOGICAL_ORDER_BY);
		node = op.children[0];
	}
}

void OptimizeWorkerFragments(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// Fragments run outside the client's transaction, so they would miss its snapshot in an explicit transaction.
	auto state = input.context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME);
	if (!state || !input.context.transaction.IsAutoCommit() || state->NumWorkers() == 0) {
		return;
	}
	vector<ReplacementBinding> replacements;
	CutFragment(input.context, input.optimizer.binder, *state, plan, /*reduced=*/false, replacements);
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

bool WorkerFragmentState::StartTasks(const WorkerFragment &fragment,
                                     vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams) {
	return executor.ExecutePlans(fragment.task_plans, streams);
}

vector<unique_ptr<DataChunk>> WorkerFragmentState::ExecuteLocally(const WorkerFragment &fragment) {
	// Materialized, as concurrent fragments share the connection and a new query would invalidate a streaming result.
	auto plan = PlanSerializer::DeserializeLogicalPlan(*connection.context, fragment.plan);
	auto result = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
	if (result->HasError()) {
		result->ThrowError();
	}
	if (result->types != fragment.types) {
		throw InternalException("Worker fragment '%s' returned unexpected types", fragment.description);
	}
	vector<unique_ptr<DataChunk>> chunks;
	for (auto chunk = result->Fetch(); chunk && chunk->size() > 0; chunk = result->Fetch()) {
		chunks.emplace_back(std::move(chunk));
	}
	return chunks;
}

void WorkerFragmentState::RecordExecution(QueryExecutionInfo info) {
	const concurrency::lock_guard<concurrency::mutex> lock(executions_mutex);
	executions.emplace_back(std::move(info));
}

bool WorkerFragmentState::IsSameCatalog(const TableCatalogEntry &table) const {
	return &table.catalog == &catalog;
}

vector<QueryExecutionInfo> WorkerFragmentState::TakeExecutions() {
	const concurrency::lock_guard<concurrency::mutex> lock(executions_mutex);
	return std::exchange(executions, {});
}

idx_t WorkerFragmentState::NumWorkers() {
	return executor.NumWorkers();
}

void RegisterJoinPartitioningSetting(DBConfig &config) {
	config.AddExtensionOption(
	    JOIN_PARTITIONING_SETTING, "How Joins are split across workers: auto (estimated costs), always or never",
	    LogicalType::VARCHAR, Value("auto"), [](ClientContext &, SetScope, Value &parameter) {
		    const auto mode = parameter.IsNull() ? string("auto") : StringUtil::Lower(parameter.ToString());
		    if (mode != "auto" && mode != "always" && mode != "never") {
			    throw InvalidInputException("%s must be 'auto', 'always' or 'never'", JOIN_PARTITIONING_SETTING);
		    }
	    });
}

void RegisterWorkerFragments(DBConfig &config) {
	// Compressed materialization bakes the driver's statistics into the plan, and workers may read newer data.
	config.options.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
	OptimizerExtension extension;
	extension.optimize_function = OptimizeWorkerFragments;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace duckdb
