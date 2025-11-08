-- Manual test to see debug logging
LOAD 'build/reldebug/repository/duckherder.duckdb_extension';

-- Enable debug logging
SET enable_profiling = true;
SET log_query_path = 'manual_test.log';

-- Start server with 2 workers
SELECT duckherder_start_local_server(8835, 2);

-- Attach distributed database
ATTACH DATABASE ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port 8835);
USE dh;

-- Register table
PRAGMA duckherder_register_remote_table('test_manual', 'test_manual');

-- Create and populate
CREATE TABLE test_manual (id INTEGER, value INTEGER);
INSERT INTO test_manual SELECT i, i * 10 FROM range(10000) t(i);

-- Run a query that should show parallelism decisions
SELECT COUNT(*), SUM(value), AVG(value) FROM test_manual;

