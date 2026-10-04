#pragma once

#include "duckdb.hpp"

#include <arrow/status.h>

namespace duckdb {

// Run the SQL in DUCKHERDER_STARTUP_SQL, if set, on a DuckDB instance. Call it on every instance the driver and workers
// create.
arrow::Status RunStartupSQL(DuckDB &db);

} // namespace duckdb
