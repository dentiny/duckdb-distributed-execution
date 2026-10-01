#pragma once

#include <arrow/flight/server.h>

namespace duckdb {

// Get an available port for binding, starting from the given port.
// Returns the first available port, or -1 if no port is available.
int GetAvailablePort(int start_port = 9000);

// Makes the server fail to start on a port that is already bound, instead of sharing it with the other listener.
void DisablePortSharing(arrow::flight::FlightServerOptions &options);

} // namespace duckdb
