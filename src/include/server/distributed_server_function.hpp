#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

// Create scalar functions for starting/stopping distributed server
ScalarFunction GetStartDistributedServerFunction();
ScalarFunction GetStopDistributedServerFunction();

} // namespace duckdb
