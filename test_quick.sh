#!/bin/bash

# Simple direct test
cd /home/vscode/duckdb-distributed-execution
rm -f md

./build/debug/extension/motherduck/distributed_server &
SERVER_PID=$!
sleep 2

./build/debug/duckdb << 'EOF'
INSTALL motherduck;
LOAD motherduck;
ATTACH DATABASE 'md' (TYPE motherduck);
CREATE TABLE md.my_table (id INTEGER, name VARCHAR, value INTEGER);
INSERT INTO md.my_table VALUES (100, 'Local', 999);

SELECT '=== Before PRAGMA ===' as step;
SELECT * FROM md.my_table;

PRAGMA md_register_remote_table('my_table', 'http://localhost:8080', 'remote_table');

SELECT '=== After PRAGMA ===' as step;
SELECT * FROM md.my_table;
EOF

kill $SERVER_PID 2>/dev/null

