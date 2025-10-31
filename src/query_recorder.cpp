#include "query_recorder.hpp"

namespace duckdb {

QueryRecorderHandle QueryRecorder::RecordQueryStart(string query) {
	return QueryRecorderHandle {*this, std::move(query)};
}

void QueryRecorder::RecordFinish(string query, uint64_t duration_millisec) {
	const std::lock_guard<std::mutex> lck(mu);
	query_timing[std::move(query)].emplace_back(duration_millisec);
}

vector<QueryRecord> QueryRecorder::GetQueryRecords() const {
	const std::lock_guard<std::mutex> lck(mu);
	vector<QueryRecord> query_records;
	query_records.reserve(query_timing.size());
	for (const auto &[query, timings] : query_timing) {
		QueryRecord cur_query_record {
		    .query = query,
		    .latencies = timings,
		};
		query_records.emplace_back(std::move(cur_query_record));
	}
	return query_records;
}

void QueryRecorder::ClearQueryRecords() {
	const std::lock_guard<std::mutex> lck(mu);
	query_timing.clear();
}

} // namespace duckdb
