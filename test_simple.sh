#!/bin/bash

# Simple test script for distributed DuckDB functionality

echo "=== Simple Distributed DuckDB Test ==="

echo "Building the project..."
make debug

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"

echo ""
echo "=== Testing Server-Side Execution ==="
echo "Starting server..."

# Start server in background
./build/debug/extension/motherduck/distributed_server &
SERVER_PID=$!

sleep 2

echo "Server started with PID: $SERVER_PID"

echo ""
echo "=== Testing Client-Side ==="
echo "Testing basic DuckDB functionality with motherduck extension:"

# Test basic functionality
./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;
ATTACH DATABASE 'md' (TYPE motherduck);

-- Test basic query
SELECT 'Hello from distributed DuckDB!' as message;
"

echo ""
echo "=== Cleanup ==="
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Test completed!"
