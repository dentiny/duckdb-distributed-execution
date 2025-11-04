#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

// Create scalar functions for starting and stopping a local server.
ScalarFunction GetStartLocalServerFunction();
ScalarFunction GetStopLocalServerFunction();

} // namespace duckdb
