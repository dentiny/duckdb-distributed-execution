#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/connection.hpp"
#include <arrow/record_batch.h>
#include <memory>

namespace duckdb {

// ResultMerger: Collects results from distributed workers.
class ResultMerger {
public:
	explicit ResultMerger(Connection &conn_p);

	// Load workers' partial aggregates into `QueryPlanAnalyzer::PARTIAL_TABLE_NAME` and run `final_sql` over them.
	unique_ptr<QueryResult> MergePartialAggregates(const vector<arrow::RecordBatchVector> &task_batches,
	                                               const vector<string> &partial_names,
	                                               const vector<LogicalType> &partial_types,
	                                               const vector<string> &output_names,
	                                               const vector<LogicalType> &output_types, const string &final_sql);

	// Concatenate worker result batches without decoding them.
	// Returns an invalid status if a worker batch does not match the schema the driver would produce for this result.
	arrow::Status CollectResults(vector<arrow::RecordBatchVector> &task_batches, const vector<string> &names,
	                             const vector<LogicalType> &types, std::shared_ptr<arrow::Schema> &schema,
	                             vector<std::shared_ptr<arrow::RecordBatch>> &batches);

private:
	Connection &conn;
};

} // namespace duckdb
