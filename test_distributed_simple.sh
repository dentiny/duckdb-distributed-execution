#!/bin/bash

# Test script for distributed DuckDB functionality (simplified version)

echo "=== Distributed DuckDB Test (Simplified) ==="

# Test client-side distributed scan with simulated remote data
echo "Testing client-side distributed scan..."
./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;
ATTACH DATABASE 'md' (TYPE motherduck);

-- Register a remote table (simulated)
CALL md.RegisterRemoteTable('remote_test_table', 'http://localhost:8080', 'remote_test_table');

-- Query the remote table (will use distributed scan)
SELECT * FROM md.remote_test_table;
"

echo "Test completed!"
