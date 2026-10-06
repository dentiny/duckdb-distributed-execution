#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
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
class RecordBatch;
class RecordBatchReader;
class Schema;
} // namespace arrow

namespace duckdb {

class ClientContext;
class DataChunk;
class QueryResult;

// Convert an Arrow type to its DuckDB logical type using DuckDB's Arrow schema importer.
LogicalType ArrowTypeToDuckDBType(ClientContext &context, const std::shared_ptr<arrow::DataType> &arrow_type);

// Convert an Arrow array to a DuckDB vector using DuckDB's Arrow scan conversion.
void ConvertArrowArrayToDuckDBVector(ClientContext &context, const std::shared_ptr<arrow::Array> &arrow_array,
                                     Vector &duckdb_vector, const LogicalType &type, idx_t num_rows);

// Convert an Arrow record batch into a DuckDB data chunk using DuckDB's Arrow scan conversion.
void ArrowRecordBatchToDataChunk(ClientContext &context, const arrow::RecordBatch &batch, DataChunk &out,
                                 const vector<LogicalType> *expected_types = nullptr);

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

// Convert a DuckDB query result into a reader over row-group-sized Arrow record batches, using `context` when the
// result does not reference a client context. Larger batches reduce per-message overhead on every network hop.
arrow::Status QueryResultToArrowReader(QueryResult &result, ClientContext &context,
                                       std::shared_ptr<arrow::RecordBatchReader> &reader, idx_t *row_count = nullptr);

} // namespace duckdb
