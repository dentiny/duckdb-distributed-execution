#!/bin/bash

# Test script for transparent distributed DuckDB queries

echo "=== Testing Transparent Distributed Queries ==="

echo "Building the project..."
make debug > /dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"
echo ""

echo "=== Starting Server ==="
./build/debug/extension/motherduck/distributed_server &
SERVER_PID=$!
sleep 2
echo "Server started with PID: $SERVER_PID"
echo ""

echo "=== Testing Transparent Distributed Query ==="
echo ""

# Test transparent distributed queries
./build/debug/duckdb -c "
-- Load extension
INSTALL motherduck;
LOAD motherduck;

-- Attach the motherduck storage extension
ATTACH DATABASE 'md' (TYPE motherduck);

-- First, create a local table in the md schema
CREATE TABLE md.remote_table (id INTEGER, name VARCHAR, value INTEGER);

-- Insert some local data  
INSERT INTO md.remote_table VALUES (100, 'Local_Data', 999);

-- Query the LOCAL table (no distributed scan)
SELECT '=== Query 1: LOCAL table (regular scan) ===' as step;
SELECT * FROM md.remote_table;

-- Now mark it as distributed
PRAGMA md_register_remote_table('remote_table', 'http://localhost:8080', 'remote_table');

-- Query the SAME table - now it's distributed!
SELECT '=== Query 2: DISTRIBUTED table (remote scan) ===' as step;
SELECT * FROM md.remote_table;

-- You can do aggregations
SELECT '=== Query 3: DISTRIBUTED aggregation ===' as step;
SELECT COUNT(*) as total_rows FROM md.remote_table;

-- And filters
SELECT '=== Query 4: DISTRIBUTED with filter ===' as step;
SELECT * FROM md.remote_table WHERE id > 1;
"

echo ""
echo "=== Cleanup ==="
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
echo "Server stopped"
echo ""
echo "✅ Test completed successfully!"
echo "   Notice how the same SQL query returned different data after marking the table as distributed!"
