#!/bin/bash

# Test script for distributed DuckDB functionality (simplified architecture)

echo "=== Distributed DuckDB Architecture Test ==="

echo "Building the project..."
make debug

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"

echo ""
echo "=== Testing Client-Side Injection ==="
echo "This demonstrates how the client intercepts table scans and routes them to distributed operators:"

./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;
ATTACH DATABASE 'md' (TYPE motherduck);

-- Register a remote table (this simulates the client-side injection)
CALL md.RegisterRemoteTable('remote_table', 'http://localhost:8080', 'remote_table');

-- Query the remote table - this will use PhysicalDistributedTableScan
SELECT * FROM md.remote_table;
"

echo ""
echo "=== Testing Server-Side Execution ==="
echo "This demonstrates how the server executes queries on its DuckDB instance:"

echo "Starting server in background..."
./build/debug/extension/motherduck/distributed_server &
SERVER_PID=$!

sleep 1

echo "Server started. You can now test the distributed architecture!"
echo "The server has a table 'remote_table' with test data."
echo "Press Ctrl+C to stop the server and exit."

# Keep the server running
wait $SERVER_PID
