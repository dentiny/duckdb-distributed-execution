#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace arrow {
class RecordBatch;
} // namespace arrow

namespace duckdb {

class ClientContext;

// Converts an Arrow record batch into DuckDB chunks using DuckDB's Arrow scan conversion. That conversion fills at most
// STANDARD_VECTOR_SIZE rows at a time, so larger batches are converted in slices.
class ArrowRecordBatchConverter {
public:
	// Converts to expected_types when given, to preserve logical types that cannot be inferred from Arrow alone.
	ArrowRecordBatchConverter(ClientContext &context, const arrow::RecordBatch &batch,
	                          const vector<LogicalType> *expected_types = nullptr);

	// The column types of the converted chunks.
	const vector<LogicalType> &GetTypes() const {
		return types;
	}

	bool HasNext() const {
		return offset < row_count;
	}

	// Converts the next rows into out, initialized with GetTypes(). Returns false once every row is converted.
	bool Next(DataChunk &out);

private:
	ClientContext &context;
	shared_ptr<ArrowArrayWrapper> owned_array;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	// Kept across slices, so each dictionary is converted once per batch.
	vector<unique_ptr<ArrowArrayScanState>> scan_states;
	// Holds the columns whose inferred type differs from the expected one before the cast; empty if there are none.
	DataChunk inferred;
	idx_t row_count;
	idx_t offset = 0;
};

} // namespace duckdb
