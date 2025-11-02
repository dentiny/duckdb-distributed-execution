#include "query_history_query_function.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "query_recorder_factory.hpp"
#include "query_recorder.hpp"
#include "time_utils.hpp"

namespace duckdb {

namespace {

struct GetQueryHistoryData : public GlobalTableFunctionState {
	vector<QueryRecord> query_records;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> GetQueryHistoryTableFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(2);
	names.reserve(2);

	return_types.emplace_back(LogicalType{LogicalTypeId::VARCHAR});
	names.emplace_back("query");

	return_types.emplace_back(LogicalType::LIST(/*child=*/LogicalType {LogicalTypeId::TIME}));
	names.emplace_back("latencies");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> GetQueryHistoryTableFuncInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<GetQueryHistoryData>();
	auto &query_records = result->query_records;
	query_records = GetQueryRecorder().GetQueryRecords();

	// Sort the results to ensure determinististism and testibility.
	std::sort(query_records.begin(), query_records.end(),
	          [](const QueryRecord &lhs, const QueryRecord &rhs) { return lhs.query < rhs.query; });

	return std::move(result);
}

void GetQueryHistoryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<GetQueryHistoryData>();

	// All entries have been emitted.
	if (data.offset >= data.query_records.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.query_records.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.query_records[data.offset++];
		idx_t col = 0;

		vector<Value> latencies;
		latencies.reserve(entry.latencies.size());
		std::transform(entry.latencies.begin(), entry.latencies.end(), std::back_inserter(latencies),
		               [](int64_t latency_millisec) {
			               const auto latency_microsec = static_cast<int64_t>(latency_millisec * kMilliToMicros);
			               return Value::TIME(dtime_t {latency_microsec});
		               });

		output.SetValue(col++, count, entry.query);
		output.SetValue(col++, count, Value::ARRAY(LogicalTypeId::TIME, std::move(latencies)));

		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetQueryHistory() {
	TableFunction list_filesystems_query_func {/*name=*/"md_get_query_history",
	                                           /*arguments=*/ {},
	                                           /*function=*/GetQueryHistoryTableFunc,
	                                           /*bind=*/GetQueryHistoryTableFuncBind,
	                                           /*init_global=*/GetQueryHistoryTableFuncInit};
	return list_filesystems_query_func;
}

} // namespace duckdb
