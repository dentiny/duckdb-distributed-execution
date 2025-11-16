#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

// Create scalar functions for starting and stopping a local server.
ScalarFunction GetStartLocalServerFunction();
ScalarFunction GetStopLocalServerFunction();

// Create scalar functions for worker management.
ScalarFunction GetWorkerCountFunction();
ScalarFunction GetStartLocalWorkersFunction();
ScalarFunction GetRegisterWorkerFunction();
ScalarFunction GetRegisterWorkersFunction();

} // namespace duckdb
