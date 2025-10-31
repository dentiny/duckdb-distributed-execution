#include "base_query_recorder.hpp"

#include "time_utils.hpp"

namespace duckdb {

QueryRecorderHandle::QueryRecorderHandle(BaseQueryRecorder &query_recorder_p, string query_p)
    : query_recorder(query_recorder_p), query(std::move(query_p)), start_timestamp(GetSteadyNowMilliSecSinceEpoch()) {
}

QueryRecorderHandle::~QueryRecorderHandle() {
	const auto now = GetSteadyNowMilliSecSinceEpoch();
	const auto latency_millisec = now - start_timestamp;
	query_recorder.RecordFinish(std::move(query), latency_millisec);
}

QueryRecorderHandle NoopQueryRecorder::RecordQueryStart(string query) {
	return QueryRecorderHandle {*this, std::move(query)};
}

void NoopQueryRecorder::RecordFinish(string query, uint64_t duration_millisec) {
}

vector<QueryRecord> NoopQueryRecorder::GetQueryRecords() const {
	return {};
}

void NoopQueryRecorder::ClearQueryRecords() {
}

} // namespace duckdb
