#!/bin/bash

# Test script for distributed DuckDB functionality

echo "=== Distributed DuckDB Test ==="

# Start the distributed server in the background
echo "Starting distributed server..."
./build/debug/distributed_server localhost 8080 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test the server health
echo "Testing server health..."
curl -s http://localhost:8080/health || echo "Server not responding"

# Create test data on the server
echo "Creating test data on server..."
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE test_table (id INTEGER, name VARCHAR, age INTEGER);"}' \
  -s > /dev/null

curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO test_table VALUES (1, \"Alice\", 30), (2, \"Bob\", 25), (3, \"Charlie\", 40);"}' \
  -s > /dev/null

echo "Test data created on server"

# Test client-side distributed scan
echo "Testing client-side distributed scan..."
./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;
ATTACH DATABASE 'md' (TYPE motherduck);

-- Register the remote table
CALL md.RegisterRemoteTable('test_table', 'http://localhost:8080', 'test_table');

-- Query the remote table
SELECT * FROM md.test_table;
"

# Clean up
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Test completed!"
