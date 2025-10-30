#include "base_query_recorder.hpp"

#include <cstdint>

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class QueryRecorder : public BaseQueryRecorder {
public:
	QueryRecorder() = default;
	~QueryRecorder() override = default;

	QueryRecorderHandle RecordQueryStart(string query) override;

private:
	void RecordFinish(string query, uint64_t duration_millisec) override;

	// Maps from query to their duration in milliseconds.
	unordered_map<string, vector<int64_t>> query_timing;
};

} // namespace duckdb
