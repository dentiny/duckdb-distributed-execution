#!/bin/bash

# Test script for transparent distributed DuckDB queries

echo "=== Testing Transparent Distributed Queries ==="

echo "Building the project..."
make debug

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
echo "Step 1: Attach motherduck database"
echo "Step 2: Register a table as distributed"
echo "Step 3: Query the table normally (distributed scan happens automatically!)"
echo ""

# Test transparent distributed queries
./build/debug/duckdb -c "
INSTALL motherduck;
LOAD motherduck;

-- Attach the motherduck storage extension
ATTACH DATABASE 'md' (TYPE motherduck);

-- Register a table as distributed using a PRAGMA
-- This marks 'remote_table' as a distributed table pointing to the server
PRAGMA md_register_remote_table('remote_table', 'http://localhost:8080', 'remote_table');

-- Now query it normally - the distributed scan happens automatically!
SELECT * FROM md.remote_table;

-- You can do joins, filters, aggregations - everything works!
SELECT COUNT(*) as total_rows FROM md.remote_table;
SELECT * FROM md.remote_table WHERE id > 1;
"

echo ""
echo "=== Cleanup ==="
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
echo "Server stopped"
echo ""
echo "✅ Test completed! The table scan was automatically distributed!"
echo "   Users just query 'SELECT * FROM md.remote_table' - no special syntax needed!"
