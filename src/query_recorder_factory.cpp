#include "query_recorder_factory.hpp"

#include "query_recorder.hpp"

namespace duckdb {

BaseQueryRecorder &GetQueryRecorder() {
	static QueryRecorder query_recorder;
	return query_recorder;
}

} // namespace duckdb
