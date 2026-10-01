#pragma once

namespace duckdb {

// Get an available port for binding, starting from the given port.
// Returns the first available port, or -1 if no port is available.
int GetAvailablePort(int start_port = 9000);

} // namespace duckdb
