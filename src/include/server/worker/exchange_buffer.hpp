#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

#include <arrow/status.h>
#include <memory>

namespace arrow {
class RecordBatchReader;
} // namespace arrow

namespace duckdb {

class DataChunk;
class ObjectStorageDatabase;

// The buckets of one exchange on a worker. One producer task appends rows and then seals the buffer; only then can
// consumer tasks on any worker read a bucket, each as often as its plan scans it, until the driver releases the
// exchange. A producer that fails never seals, so its partial rows cannot be read.
class ExchangeBuffer {
public:
	// Rows are kept in database's buffer manager, so they count toward its memory limit and can spill.
	ExchangeBuffer(ObjectStorageDatabase &database, vector<LogicalType> types, idx_t bucket_count);

	const vector<LogicalType> &GetTypes() const {
		return types;
	}
	idx_t BucketCount() const {
		return buckets.size();
	}
	std::chrono::steady_clock::time_point CreatedAt() const {
		return created_at;
	}

	// Adds each row of chunk, whose last column is its bucket, to that bucket without the bucket column. Returns the
	// number of rows added to each bucket. Only the producer calls it, before Seal.
	vector<idx_t> Append(DataChunk &chunk);
	// Makes the buckets readable; they do not change afterwards.
	void Seal();

	// Streams the rows of bucket as Arrow batches. The reader keeps buffer alive. Fails if buffer is not sealed.
	static arrow::Status ReadBucket(shared_ptr<ExchangeBuffer> buffer, idx_t bucket,
	                                std::shared_ptr<arrow::RecordBatchReader> &reader);

private:
	ObjectStorageDatabase &database;
	vector<LogicalType> types;
	const std::chrono::steady_clock::time_point created_at;
	vector<unique_ptr<ColumnDataCollection>> buckets;
	// Set by the producer after its last Append, so readers see every row without locking.
	atomic<bool> sealed {false};
};

} // namespace duckdb
