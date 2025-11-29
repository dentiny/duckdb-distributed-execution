#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Wait until a driver server is ready, with polling and timeout.
// Returns whether the server becomes ready within the timeout.
bool WaitForDriverServerReady(const string &location);

// Wait until a worker node is ready, with polling and timeout.
// Returns whether the worker becomes ready within the timeout.
bool WaitForWorkerNodeReady(const string &location);

} // namespace duckdb
