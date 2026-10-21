#include "server/worker/exchange_buffer.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "server/object_storage_database.hpp"

#include <arrow/record_batch.h>

namespace duckdb {

ExchangeBuffer::ExchangeBuffer(ObjectStorageDatabase &database_p, vector<LogicalType> types_p, idx_t bucket_count)
    : database(database_p), types(std::move(types_p)), created_at(std::chrono::steady_clock::now()) {
	auto &buffer_manager = BufferManager::GetBufferManager(*database.GetInstance().instance);
	buckets.reserve(bucket_count);
	for (idx_t bucket = 0; bucket < bucket_count; ++bucket) {
		buckets.push_back(make_uniq<ColumnDataCollection>(buffer_manager, types));
	}
}

vector<idx_t> ExchangeBuffer::Append(DataChunk &chunk) {
	D_ASSERT(!sealed);
	D_ASSERT(chunk.ColumnCount() == types.size() + 1);
	const auto bucket_count = BucketCount();
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

void ExchangeBuffer::Seal() {
	sealed = true;
}

arrow::Status ExchangeBuffer::ReadBucket(shared_ptr<ExchangeBuffer> buffer, idx_t bucket,
                                         std::shared_ptr<arrow::RecordBatchReader> &reader) {
	if (!buffer->sealed) {
		return arrow::Status::Invalid("Exchange is still being written or its producer failed");
	}
	if (bucket >= buffer->BucketCount()) {
		return arrow::Status::Invalid("Exchange has ", buffer->BucketCount(), " buckets, not bucket ", bucket);
	}
	// Arrow conversion needs a client context.
	ARROW_ASSIGN_OR_RAISE(auto connection, buffer->database.Connect());
	auto client_properties = connection->context->GetClientProperties();
	vector<string> names;
	names.reserve(buffer->types.size());
	for (idx_t column_idx = 0; column_idx < buffer->types.size(); ++column_idx) {
		names.push_back(StringUtil::Format("c%llu", column_idx));
	}
	auto &collection = *buffer->buckets[bucket];
	auto scan_state = make_shared_ptr<ColumnDataScanState>();
	collection.InitializeScan(*scan_state);
	// Sealed buckets do not change, so readers of one bucket run concurrently without locking. The fetch keeps buffer
	// alive after the exchange is released.
	return ChunksToArrowReader(std::move(connection), collection.Types(), names, std::move(client_properties),
	                           [buffer, &collection, scan_state]() {
		                           auto chunk = make_uniq<DataChunk>();
		                           collection.InitializeScanChunk(*chunk);
		                           collection.Scan(*scan_state, *chunk);
		                           return chunk;
	                           },
	                           reader);
}

} // namespace duckdb
