// Class which records the runtime information of queries.

#pragma once

#include <cstdint>

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Forward declaration.
class BaseQueryRecorder;

// A RAII handle for query recorder.
class QueryRecorderHandle {
public:
	QueryRecorderHandle(BaseQueryRecorder &query_recorder_p, string query_p);
	~QueryRecorderHandle();

private:
	BaseQueryRecorder &query_recorder;
	string query;
	int64_t start_timestamp = 0;
};

// Query record for single statement execution.
struct QueryRecord {
	string query;
	vector<int64_t> latencies;
};

class BaseQueryRecorder {
public:
	BaseQueryRecorder() = default;
	virtual ~BaseQueryRecorder() = default;

	// Record the start of the query.
	virtual QueryRecorderHandle RecordQueryStart(string query) = 0;

	// Get all query stats.
	virtual vector<QueryRecord> GetQueryRecords() const = 0;

	// Clear all query stats.
	virtual void ClearQueryRecords() = 0;

private:
	friend QueryRecorderHandle;

	// Mark the completion of the query.
	virtual void RecordFinish(string query, uint64_t duration_millisec) = 0;
};

class NoopQueryRecorder : public BaseQueryRecorder {
public:
	NoopQueryRecorder() = default;
	~NoopQueryRecorder() override = default;

	QueryRecorderHandle RecordQueryStart(string query) override;

	vector<QueryRecord> GetQueryRecords() const override;

	void ClearQueryRecords() override;

private:
	void RecordFinish(string query, uint64_t duration_millisec) override;
};

} // namespace duckdb
