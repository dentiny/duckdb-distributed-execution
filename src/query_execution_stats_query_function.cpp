#include "query_execution_stats_query_function.hpp"

#include "client/distributed_client.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

namespace {

struct QueryExecutionStatsData : public GlobalTableFunctionState {
	// Stats data: (sql, execution_mode, merge_strategy, query_duration_ms, num_workers_used, num_tasks_generated, execution_start_time_ms)
	vector<std::tuple<string, string, string, int64_t, int64_t, int64_t, int64_t>> query_stats;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> QueryExecutionStatsTableFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	// Define the schema for the result table.
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

	// Get client instance and fetch stats via gRPC
	auto &client = DistributedClient::GetInstance();
	auto query_result = client.GetQueryExecutionStats(result->query_stats);
	
	// If there was an error fetching stats (e.g., server not available),
	// return empty stats - the vector is already empty by default
	// This allows the function to work gracefully without the server

	return std::move(result);
}

void QueryExecutionStatsTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<QueryExecutionStatsData>();

	// All entries have been emitted.
	if (data.offset >= data.query_stats.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.query_stats.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &entry = data.query_stats[data.offset++];
		idx_t col = 0;

		// SQL query
		output.SetValue(col++, count, Value(std::get<0>(entry)));

		// Execution mode (partitioning strategy)
		output.SetValue(col++, count, Value(std::get<1>(entry)));

		// Merge strategy
		output.SetValue(col++, count, Value(std::get<2>(entry)));

		// Query duration in milliseconds
		output.SetValue(col++, count, Value::BIGINT(std::get<3>(entry)));

		// Number of workers used
		output.SetValue(col++, count, Value::BIGINT(std::get<4>(entry)));

		// Number of tasks generated
		output.SetValue(col++, count, Value::BIGINT(std::get<5>(entry)));

		// Execution start time as timestamp (convert from milliseconds to microseconds)
		auto milliseconds_since_epoch = std::get<6>(entry);
		auto microseconds_since_epoch = milliseconds_since_epoch * 1000;
		output.SetValue(col++, count, Value::TIMESTAMP(timestamp_t(microseconds_since_epoch)));

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
