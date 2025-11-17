#include "query_execution_stats_query_function.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "server/driver/distributed_flight_server.hpp"

namespace duckdb {

// Forward declaration - this is defined in distributed_server_function.cpp
extern unique_ptr<DistributedFlightServer> g_test_server;

namespace {

struct QueryExecutionStatsData : public GlobalTableFunctionState {
	vector<QueryExecutionInfo> query_executions;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

// Helper function to convert execution mode to string
string ExecutionModeToString(QueryExecutionMode mode) {
	switch (mode) {
	case QueryExecutionMode::DELEGATED:
		return "DELEGATED";
	case QueryExecutionMode::NATURAL_PARTITION:
		return "NATURAL_PARTITION";
	case QueryExecutionMode::ROW_GROUP_PARTITION:
		return "ROW_GROUP_PARTITION";
	default:
		return "UNKNOWN";
	}
}

// Helper function to convert merge strategy to string
string MergeStrategyToString(QueryPlanAnalyzer::MergeStrategy strategy) {
	switch (strategy) {
	case QueryPlanAnalyzer::MergeStrategy::CONCATENATE:
		return "CONCATENATE";
	case QueryPlanAnalyzer::MergeStrategy::AGGREGATE_MERGE:
		return "AGGREGATE";
	case QueryPlanAnalyzer::MergeStrategy::GROUP_BY_MERGE:
		return "GROUP_BY";
	case QueryPlanAnalyzer::MergeStrategy::DISTINCT_MERGE:
		return "DISTINCT";
	default:
		return "UNKNOWN";
	}
}

unique_ptr<FunctionData> QueryExecutionStatsTableFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	// Define the schema for the result table
	return_types.reserve(7);
	names.reserve(7);

	// SQL query
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sql");

	// Execution mode (partitioning strategy)
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_mode");

	// Merge strategy
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("merge_strategy");

	// Total query duration in milliseconds
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("query_duration_ms");

	// Number of workers used
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("num_workers_used");

	// Number of tasks generated
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("num_tasks_generated");

	// Execution start timestamp
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("execution_start_time");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> QueryExecutionStatsTableFuncInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto result = make_uniq<QueryExecutionStatsData>();

	// Check if server is running
	if (g_test_server == nullptr) {
		// Return empty results if server is not started
		return std::move(result);
	}

	// Get query executions from the server
	auto &query_executions = result->query_executions;
	query_executions = g_test_server->GetQueryExecutions();

	return std::move(result);
}

void QueryExecutionStatsTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<QueryExecutionStatsData>();

	// All entries have been emitted.
	if (data.offset >= data.query_executions.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.query_executions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.query_executions[data.offset++];
		idx_t col = 0;

		// SQL query
		output.SetValue(col++, count, Value(entry.sql));

		// Execution mode (partitioning strategy)
		output.SetValue(col++, count, Value(ExecutionModeToString(entry.execution_mode)));

		// Merge strategy
		output.SetValue(col++, count, Value(MergeStrategyToString(entry.merge_strategy)));

		// Query duration in milliseconds
		output.SetValue(col++, count, Value::BIGINT(entry.query_duration.count()));

		// Number of workers used
		output.SetValue(col++, count, Value::BIGINT(entry.num_workers_used));

		// Number of tasks generated
		output.SetValue(col++, count, Value::BIGINT(entry.num_tasks_generated));

		// Execution start time as timestamp
		auto time_since_epoch = entry.execution_start_time.time_since_epoch();
		auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time_since_epoch).count();
		output.SetValue(col++, count, Value::TIMESTAMP(timestamp_t(microseconds)));

		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetQueryExecutionStats() {
	TableFunction query_exec_stats_func {/*name=*/"duckherder_get_query_execution_stats",
	                                     /*arguments=*/ {},
	                                     /*function=*/QueryExecutionStatsTableFunc,
	                                     /*bind=*/QueryExecutionStatsTableFuncBind,
	                                     /*init_global=*/QueryExecutionStatsTableFuncInit};
	return query_exec_stats_func;
}

} // namespace duckdb
