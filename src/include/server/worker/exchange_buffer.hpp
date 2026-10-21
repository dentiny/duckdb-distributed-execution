#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "utils/mutex.hpp"

#include <arrow/status.h>
#include <memory>

namespace arrow {
class RecordBatchReader;
} // namespace arrow

namespace duckdb {

class DataChunk;
class ObjectStorageDatabase;

// The buckets of one exchange on a worker: producer tasks add rows, and consumer tasks on any worker read a bucket,
// each as often as its plan scans it, until the driver releases the exchange.
class ExchangeBuffer {
public:
	// Rows are kept in database's buffer manager, so they count toward its memory limit and can spill.
	ExchangeBuffer(ObjectStorageDatabase &database, vector<LogicalType> types, idx_t bucket_count);

	const vector<LogicalType> &GetTypes() const {
		return types;
	}
	idx_t BucketCount() const {
		return bucket_count;
	}

	// Adds each row of chunk, whose last column is its bucket, to that bucket without the bucket column. Returns the
	// number of rows added to each bucket.
	vector<idx_t> Append(DataChunk &chunk);

	// Streams the rows of bucket as Arrow batches. The reader keeps buffer alive.
	static arrow::Status ReadBucket(shared_ptr<ExchangeBuffer> buffer, idx_t bucket,
	                                std::shared_ptr<arrow::RecordBatchReader> &reader);

private:
	ObjectStorageDatabase &database;
	vector<LogicalType> types;
	const idx_t bucket_count;
	concurrency::mutex mu;
	// Only appended to while producers run; read after the driver has seen every producer finish.
	vector<unique_ptr<ColumnDataCollection>> buckets DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb
