#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table/arrow.hpp"

#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>

// Throw a DuckDB IOException carrying the error message if `expr`, an arrow::Status or arrow::Result, is not OK.
#define ARROW_THROW_IF_ERROR(expr)                                                                                     \
	do {                                                                                                               \
		::arrow::Status _s = ::arrow::ToStatus(expr);                                                                  \
		if (ARROW_PREDICT_FALSE(!_s.ok())) {                                                                           \
			throw ::duckdb::IOException("%s failed: %s", ARROW_STRINGIFY(expr), _s.ToString());                        \
		}                                                                                                              \
	} while (false)

namespace arrow {
class Array;
class DataType;
class RecordBatch;
class RecordBatchReader;
class Schema;
} // namespace arrow

namespace duckdb {

class ClientContext;
class Connection;
class DataChunk;
class QueryResult;

// Convert an Arrow type to its DuckDB logical type using DuckDB's Arrow schema importer.
LogicalType ArrowTypeToDuckDBType(ClientContext &context, const std::shared_ptr<arrow::DataType> &arrow_type);

// Convert an Arrow array to a DuckDB vector using DuckDB's Arrow scan conversion.
void ConvertArrowArrayToDuckDBVector(ClientContext &context, const std::shared_ptr<arrow::Array> &arrow_array,
                                     Vector &duckdb_vector, const LogicalType &type, idx_t num_rows);

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

// Convert Arrow record batches into a materialized DuckDB query result, releasing each batch once it is converted.
// Uses expected_types when provided to preserve logical types that cannot be inferred from Arrow alone.
unique_ptr<QueryResult> MakeArrowResult(ClientContext &context, StatementType statement_type,
                                        vector<std::shared_ptr<arrow::RecordBatch>> batches,
                                        const std::shared_ptr<arrow::Schema> &schema,
                                        const vector<LogicalType> *expected_types);

// Convert a DuckDB query result into Arrow record batches, one per result chunk.
// The result's client properties must reference a client context.
arrow::Status QueryResultToArrowBatches(QueryResult &result, std::shared_ptr<arrow::Schema> &schema,
                                        vector<std::shared_ptr<arrow::RecordBatch>> &batches);

// Convert a DuckDB query result of connection into a reader over row-group-sized Arrow record batches, fetched from the
// result as they are read. Larger batches reduce per-message overhead on every network hop.
arrow::Status QueryResultToArrowReader(unique_ptr<Connection> connection, unique_ptr<QueryResult> result,
                                       std::shared_ptr<arrow::RecordBatchReader> &reader);

} // namespace duckdb
