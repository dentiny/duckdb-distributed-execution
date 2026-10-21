#include "server/worker/exchange_buffer.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "server/object_storage_database.hpp"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

namespace duckdb {

namespace {

// Feeds a bucket to ArrowUtil one chunk at a time. Each chunk is scanned under the buffer's lock, so readers of one
// bucket can run concurrently.
class BucketScanState : public ChunkScanState {
public:
	BucketScanState(concurrency::mutex &mu_p, ColumnDataCollection &bucket_p, vector<string> names_p)
	    : mu(mu_p), bucket(bucket_p), names(std::move(names_p)) {
		bucket.InitializeScan(scan_state);
	}

	// Returns false only on errors, which bucket scans do not have; the chunk is empty once the bucket is read.
	bool LoadNextChunk(ErrorData &error) override {
		if (!current_chunk) {
			current_chunk = make_uniq<DataChunk>();
			bucket.InitializeScanChunk(*current_chunk);
		}
		offset = 0;
		if (finished) {
			current_chunk->Reset();
			return true;
		}
		{
			const concurrency::lock_guard<concurrency::mutex> lock(mu);
			bucket.Scan(scan_state, *current_chunk);
		}
		finished = current_chunk->size() == 0;
		return true;
	}
	bool HasError() const override {
		return false;
	}
	ErrorData &GetError() override {
		throw InternalException("Exchange bucket scans do not fail");
	}
	const vector<LogicalType> &Types() const override {
		return bucket.Types();
	}
	const vector<string> &Names() const override {
		return names;
	}

private:
	concurrency::mutex &mu;
	ColumnDataCollection &bucket;
	vector<string> names;
	ColumnDataScanState scan_state;
};

class BucketArrowReader : public arrow::RecordBatchReader {
public:
	BucketArrowReader(shared_ptr<ExchangeBuffer> buffer_p, unique_ptr<Connection> connection_p,
	                  unique_ptr<BucketScanState> scan_state_p, ClientProperties client_properties_p,
	                  std::shared_ptr<arrow::Schema> schema_p)
	    : buffer(std::move(buffer_p)), connection(std::move(connection_p)), scan_state(std::move(scan_state_p)),
	      client_properties(std::move(client_properties_p)), schema_(std::move(schema_p)) {
	}

	std::shared_ptr<arrow::Schema> schema() const override {
		return schema_;
	}

	arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
		ArrowArray arrow_array;
		idx_t fetched = 0;
		ErrorData error;
		if (!ArrowUtil::TryFetchChunk(*scan_state, client_properties, DEFAULT_ROW_GROUP_SIZE, &arrow_array, fetched,
		                              error, {})) {
			return arrow::Status::Invalid(error.Message());
		}
		if (fetched == 0) {
			batch->reset();
			return arrow::Status::OK();
		}
		ARROW_ASSIGN_OR_RAISE(*batch, arrow::ImportRecordBatch(&arrow_array, schema_));
		return arrow::Status::OK();
	}

private:
	shared_ptr<ExchangeBuffer> buffer;
	unique_ptr<Connection> connection;
	unique_ptr<BucketScanState> scan_state;
	ClientProperties client_properties;
	std::shared_ptr<arrow::Schema> schema_;
};

} // namespace

ExchangeBuffer::ExchangeBuffer(ObjectStorageDatabase &database_p, vector<LogicalType> types_p, idx_t bucket_count_p)
    : database(database_p), types(std::move(types_p)), bucket_count(bucket_count_p) {
	auto &buffer_manager = BufferManager::GetBufferManager(*database.GetInstance().instance);
	buckets.reserve(bucket_count);
	for (idx_t bucket = 0; bucket < bucket_count; ++bucket) {
		buckets.push_back(make_uniq<ColumnDataCollection>(buffer_manager, types));
	}
}

vector<idx_t> ExchangeBuffer::Append(DataChunk &chunk) {
	D_ASSERT(chunk.ColumnCount() == types.size() + 1);
	vector<SelectionVector> selections(bucket_count);
	vector<idx_t> counts(bucket_count, 0);
	for (auto &selection : selections) {
		selection.Initialize(chunk.size());
	}
	UnifiedVectorFormat bucket_format;
	chunk.data.back().ToUnifiedFormat(chunk.size(), bucket_format);
	auto bucket_data = UnifiedVectorFormat::GetData<int32_t>(bucket_format);
	for (idx_t row_idx = 0; row_idx < chunk.size(); ++row_idx) {
		const auto idx = bucket_format.sel->get_index(row_idx);
		if (!bucket_format.validity.RowIsValid(idx) || bucket_data[idx] < 0 ||
		    NumericCast<idx_t>(bucket_data[idx]) >= bucket_count) {
			throw InternalException("Exchange row has no valid bucket");
		}
		const auto bucket = NumericCast<idx_t>(bucket_data[idx]);
		selections[bucket].set_index(counts[bucket]++, row_idx);
	}

	DataChunk rows;
	rows.InitializeEmpty(types);
	const concurrency::lock_guard<concurrency::mutex> lock(mu);
	for (idx_t bucket = 0; bucket < bucket_count; ++bucket) {
		if (counts[bucket] == 0) {
			continue;
		}
		for (idx_t column_idx = 0; column_idx < types.size(); ++column_idx) {
			rows.data[column_idx].Slice(chunk.data[column_idx], selections[bucket], counts[bucket]);
		}
		rows.SetCardinality(counts[bucket]);
		buckets[bucket]->Append(rows);
	}
	return counts;
}

arrow::Status ExchangeBuffer::ReadBucket(shared_ptr<ExchangeBuffer> buffer, idx_t bucket,
                                         std::shared_ptr<arrow::RecordBatchReader> &reader) {
	if (bucket >= buffer->BucketCount()) {
		return arrow::Status::Invalid("Exchange has ", buffer->BucketCount(), " buckets, not bucket ", bucket);
	}
	// Arrow conversion needs a client context.
	ARROW_ASSIGN_OR_RAISE(auto connection, buffer->database.Connect());
	ClientProperties client_properties = connection->context->GetClientProperties();
	client_properties.arrow_lossless_conversion = true;
	vector<string> names;
	names.reserve(buffer->types.size());
	for (idx_t column_idx = 0; column_idx < buffer->types.size(); ++column_idx) {
		names.push_back(StringUtil::Format("c%llu", column_idx));
	}
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, buffer->types, names, client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));
	ColumnDataCollection *collection;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(buffer->mu);
		collection = buffer->buckets[bucket].get();
	}
	auto scan_state = make_uniq<BucketScanState>(buffer->mu, *collection, std::move(names));
	reader = std::make_shared<BucketArrowReader>(std::move(buffer), std::move(connection), std::move(scan_state),
	                                             std::move(client_properties), std::move(schema));
	return arrow::Status::OK();
}

} // namespace duckdb
