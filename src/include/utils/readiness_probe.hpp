#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Check if a driver server is ready by attempting to connect and call ListFlights.
// Returns true if the server is ready, false otherwise.
bool CheckDriverServerReady(const string &location);

// Check if a worker node is ready by attempting to connect and call WorkerHeartbeat action.
// Returns true if the worker is ready, false otherwise.
bool CheckWorkerNodeReady(const string &location);

// Wait until a driver server is ready, with polling and timeout.
// Returns true if the server becomes ready within the timeout, false otherwise.
bool WaitForDriverServerReady(const string &location);

// Wait until a worker node is ready, with polling and timeout.
// Returns true if the worker becomes ready within the timeout, false otherwise.
bool WaitForWorkerNodeReady(const string &location);

} // namespace duckdb
