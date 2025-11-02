#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"

#include <arrow/array.h>
#include <arrow/type.h>
#include <memory>

namespace duckdb {

// Convert Arrow type to DuckDB LogicalType.
// Returns VARCHAR for unsupported types as a fallback.
LogicalType ArrowTypeToDuckDBType(const std::shared_ptr<arrow::DataType> &arrow_type);

// Convert Arrow array data to DuckDB vector.
void ConvertArrowArrayToDuckDBVector(const std::shared_ptr<arrow::Array> &arrow_array, Vector &duckdb_vector,
                                     const LogicalType &type, idx_t num_rows);

} // namespace duckdb

