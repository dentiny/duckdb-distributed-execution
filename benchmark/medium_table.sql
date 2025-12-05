.timer on

-- ========================================
-- BENCHMARK 2: Medium Table (200,000 rows)
-- ========================================

.print '========================================'
.print 'Test 2A: Baseline (No Distribution)'
.print '========================================'

CREATE TABLE medium_table_baseline (id INTEGER, value INTEGER, category VARCHAR, amount DECIMAL(10,2));

INSERT INTO medium_table_baseline 
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
FROM range(200000) t(i);

SELECT category, COUNT(*), SUM(value), AVG(amount) 
FROM medium_table_baseline 
GROUP BY category;

.print ''
.print '========================================'
.print 'Test 2B: Distributed (4 Workers)'
.print '========================================'

LOAD 'build/reldebug/extension/duckherder/duckherder.duckdb_extension';
SELECT duckherder_start_local_server(8831, 4);
ATTACH DATABASE ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port 8831);
USE dh;
PRAGMA duckherder_register_remote_table('medium_table', 'medium_table');

CREATE TABLE medium_table (id INTEGER, value INTEGER, category VARCHAR, amount DECIMAL(10,2));

INSERT INTO medium_table 
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
FROM range(200000) t(i);

SELECT category, COUNT(*), SUM(value), AVG(amount) 
FROM medium_table 
GROUP BY category;

.print ''
.print 'Execution Stats:'
SELECT execution_mode, query_duration_ms, num_workers_used, num_tasks_generated 
FROM duckherder_get_query_execution_stats() 
ORDER BY execution_start_time DESC LIMIT 1;
