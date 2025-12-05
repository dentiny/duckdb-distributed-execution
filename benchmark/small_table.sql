.timer on

-- ========================================
-- BENCHMARK 1: Small Table (1,000 rows)
-- ========================================

.print '========================================'
.print 'Test 1A: Baseline (No Distribution)'
.print '========================================'

CREATE TABLE small_table_baseline (id INTEGER, value INTEGER, category VARCHAR, amount DECIMAL(10,2));

INSERT INTO small_table_baseline 
SELECT 
    i AS id, i * 100 AS value,
    CASE (i % 5)
        WHEN 0 THEN 'electronics' 
        WHEN 1 THEN 'clothing'
        WHEN 2 THEN 'food' 
        WHEN 3 THEN 'books' 
        ELSE 'furniture'
    END AS category,
    (i * 1.5)::DECIMAL(10,2) AS amount
FROM range(1000) t(i);

SELECT COUNT(*), SUM(value), AVG(amount) FROM small_table_baseline;

.print ''
.print '========================================'
.print 'Test 1B: Distributed (4 Workers)'
.print '========================================'

LOAD 'build/reldebug/extension/duckherder/duckherder.duckdb_extension';
SELECT duckherder_start_local_server(8830, 4);
ATTACH DATABASE ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port 8830);
USE dh;
PRAGMA duckherder_register_remote_table('small_table', 'small_table');

CREATE TABLE small_table (id INTEGER, value INTEGER, category VARCHAR, amount DECIMAL(10,2));

INSERT INTO small_table 
SELECT 
    i AS id, i * 100 AS value,
    CASE (i % 5)
        WHEN 0 THEN 'electronics' 
        WHEN 1 THEN 'clothing'
        WHEN 2 THEN 'food' 
        WHEN 3 THEN 'books' 
        ELSE 'furniture'
    END AS category,
    (i * 1.5)::DECIMAL(10,2) AS amount
FROM range(1000) t(i);

SELECT COUNT(*), SUM(value), AVG(amount) FROM small_table;

.print ''
.print 'Execution Stats:'
SELECT execution_mode, query_duration_ms, num_workers_used, num_tasks_generated 
FROM duckherder_get_query_execution_stats() 
ORDER BY execution_start_time DESC LIMIT 1;
