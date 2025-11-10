#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include <arrow/flight/client.h>
#include <memory>

namespace duckdb {

// ResultMerger: Collects and merges results from distributed workers.
// Applies intelligent merge strategies based on query type.
class ResultMerger {
public:
	explicit ResultMerger(Connection &conn_p);

	// Collect and merge results from worker streams (simple concatenation).
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

	// Collect and merge results with smart merging based on query analysis.
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types,
	                                               const QueryPlanAnalyzer::QueryAnalysis &query_analysis);

	// Build SQL to re-aggregate partial aggregates (no GROUP BY).
	static string BuildAggregateMergeSQL(const string &temp_table, const vector<string> &column_names,
	                                     const QueryPlanAnalyzer::QueryAnalysis &analysis);

	// Build SQL to re-group and re-aggregate (with GROUP BY).
	static string BuildGroupByMergeSQL(const string &temp_table, const vector<string> &column_names,
	                                   const QueryPlanAnalyzer::QueryAnalysis &analysis);

private:
	Connection &conn;
};

} // namespace duckdb
