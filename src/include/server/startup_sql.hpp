#pragma once

#include "duckdb.hpp"

#include <arrow/status.h>

namespace duckdb {

// Run the SQL in DUCKHERDER_STARTUP_SQL, if set, on a DuckDB instance, e.g. to change settings or wrap filesystems for
// an experiment. Call it on every instance the driver and workers create.
arrow::Status RunStartupSQL(DuckDB &db);

} // namespace duckdb
