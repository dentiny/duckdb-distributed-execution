#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Row ID filters do not prune row groups in this DuckDB version, so a worker scanning its partition of a table, a
// row ID range, would read every row group. This replaces such table scans with one restricted to the row groups the
// row ID filter can match.
OptimizerExtension GetRowGroupRangeScanExtension();

} // namespace duckdb
