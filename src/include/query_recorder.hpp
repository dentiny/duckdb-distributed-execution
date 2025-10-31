#include "base_query_recorder.hpp"

#include <cstdint>
#include <mutex>

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class QueryRecorder : public BaseQueryRecorder {
public:
	QueryRecorder() = default;
	~QueryRecorder() override = default;

	QueryRecorderHandle RecordQueryStart(string query) override;

	vector<QueryRecord> GetQueryRecords() const override;

private:
	void RecordFinish(string query, uint64_t duration_millisec) override;

	mutable std::mutex mu;
	// Maps from query to their duration in milliseconds.
	// TODO(hjiang): Add other metrics.
	unordered_map<string, vector<int64_t>> query_timing;
};

} // namespace duckdb
