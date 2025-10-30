#include "query_recorder.hpp"

namespace duckdb {

QueryRecorderHandle QueryRecorder::RecordQueryStart(string query) {
	return QueryRecorderHandle {*this, std::move(query)};
}

void QueryRecorder::RecordFinish(string query, uint64_t duration_millisec) {
	query_timing[std::move(query)].emplace_back(duration_millisec);
}

} // namespace duckdb
