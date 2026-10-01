#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"

#include <arrow/array.h>
#include <arrow/type.h>
#include <memory>

namespace arrow {
class RecordBatch;
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

} // namespace duckdb
