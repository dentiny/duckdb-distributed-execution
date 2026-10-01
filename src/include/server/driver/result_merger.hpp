#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types.hpp"
#include <arrow/flight/client.h>
#include <memory>

namespace duckdb {

// ResultMerger: Collects results from distributed workers.
class ResultMerger {
public:
	explicit ResultMerger(Connection &conn_p);

	// Collect and merge results from worker streams (simple concatenation).
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

private:
	Connection &conn;
};

} // namespace duckdb
