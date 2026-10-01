#include "server/driver/result_merger.hpp"
#include "arrow_utils.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

ResultMerger::ResultMerger(Connection &conn_p) : conn(conn_p) {
}

unique_ptr<QueryResult>
ResultMerger::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                     const vector<string> &names, const vector<LogicalType> &types) {
	// Coordinator acts as GlobalState aggregator in DuckDB's parallel execution model
	//
	// DuckDB's parallel execution pattern:
	// 1. Multiple threads execute in parallel, each with LocalSinkState
	// 2. Combine() merges LocalSinkState into GlobalSinkState
	// 3. Finalize() produces the final result from GlobalSinkState
	//
	// Distributed execution mapping:
	// 1. Multiple worker nodes execute in parallel (each = one thread)
	// 2. Each worker returns LocalState output (as Arrow RecordBatches)
	// 3. This method performs the Combine() operation:
	//    - Collects LocalState outputs from all workers
	//    - Merges them into a unified result (GlobalState)
	// 4. The ColumnDataCollection acts as our GlobalSinkState
	//
	// This maintains the same aggregation semantics as thread-level parallelism,
	// but distributed across network-connected nodes.
	//
	// Collection will be created lazily after we see the first batch's schema
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> actual_types; // Types from actual Arrow data

	// Combine phase: Merge LocalState outputs from each worker
	idx_t worker_idx = 0;
	idx_t total_batches = 0;
	idx_t total_rows_combined = 0;

	for (auto &stream : streams) {
		idx_t worker_batches = 0;
		idx_t worker_rows = 0;

		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				throw IOException("Failed reading worker result: %s", batch_result.status().ToString());
			}

			auto batch_with_metadata = batch_result.ValueOrDie();
			if (!batch_with_metadata.data) {
				break; // End of stream from this worker
			}

			// Convert Arrow batch (LocalState output) to DuckDB DataChunk
			auto arrow_batch = batch_with_metadata.data;
			DataChunk chunk;
			ArrowRecordBatchToDataChunk(*conn.context, *arrow_batch, chunk, types.empty() ? nullptr : &types);

			// Initialize collection with actual schema from first batch
			if (!collection) {
				actual_types = chunk.GetTypes();
				collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), actual_types);
			}

			// Append to GlobalSinkState (ColumnDataCollection)
			collection->Append(chunk);

			worker_batches++;
			worker_rows += arrow_batch->num_rows();
			total_batches++;
			total_rows_combined += arrow_batch->num_rows();
		}
		worker_idx++;
	}

	// Finalize phase: Return the aggregated result
	// In this simple case, we just return the merged collection
	// For more complex operators (aggregates, sorts, etc.), additional
	// finalization logic would go here (e.g., final aggregation, final sort)
	if (!collection) {
		collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties {}, names,
	                                          std::move(collection), ClientProperties {});
}

} // namespace duckdb
