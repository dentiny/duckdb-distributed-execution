#pragma once

namespace duckdb {

// Network utility functions for distributed execution

// Get an available port for binding, starting from the given port
// Returns the first available port, or -1 if no port is available
int GetAvailablePort(int start_port = 9000, int max_attempts = 1000);

} // namespace duckdb

