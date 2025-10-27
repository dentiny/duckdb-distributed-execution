#!/bin/bash

# Test script for distributed DuckDB table function

echo "=== Testing Distributed Table Function ==="

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
echo "=== Testing Distributed Table Function ==="
echo "Testing the distributed_table function:"

# Test the distributed table function
./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;

-- Test the distributed table function
SELECT * FROM distributed_table('http://localhost:8080', 'remote_table');
"

echo ""
echo "=== Testing with Different Parameters ==="
echo "Testing with different server URLs:"

# Test with different parameters
./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;

-- Test with different parameters
SELECT * FROM distributed_table('http://server1:8080', 'users');
"

echo ""
echo "=== Cleanup ==="
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Test completed!"
