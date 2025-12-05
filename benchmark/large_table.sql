.timer on

-- ========================================
-- BENCHMARK 3: Large Table (500,000 rows)
-- ========================================

.print '========================================'
.print 'Test 3A: Baseline (No Distribution)'
.print '========================================'

CREATE TABLE large_table_baseline (id INTEGER, value INTEGER, category VARCHAR, description VARCHAR);

INSERT INTO large_table_baseline 
SELECT 
    i AS id, i * 100 AS value,
    CASE (i % 5)
        WHEN 0 THEN 'electronics' 
        WHEN 1 THEN 'clothing'
        WHEN 2 THEN 'food' 
        WHEN 3 THEN 'books' 
        ELSE 'other'
    END AS category,
    'Item ' || i::VARCHAR AS description
FROM range(500000) t(i);

SELECT category, COUNT(*) as cnt, SUM(value) as total_value 
FROM large_table_baseline 
GROUP BY category;

.print ''
.print '========================================'
.print 'Test 3B: Distributed (4 Workers)'
.print '========================================'

LOAD 'build/reldebug/extension/duckherder/duckherder.duckdb_extension';
SELECT duckherder_start_local_server(8832, 4);
ATTACH DATABASE ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port 8832);
USE dh;
PRAGMA duckherder_register_remote_table('large_table', 'large_table');

CREATE TABLE large_table (id INTEGER, value INTEGER, category VARCHAR, description VARCHAR);

INSERT INTO large_table 
SELECT 
    i AS id, i * 100 AS value,
    CASE (i % 5)
        WHEN 0 THEN 'electronics' 
        WHEN 1 THEN 'clothing'
        WHEN 2 THEN 'food' 
        WHEN 3 THEN 'books' 
        ELSE 'other'
    END AS category,
    'Item ' || i::VARCHAR AS description
FROM range(500000) t(i);

SELECT category, COUNT(*) as cnt, SUM(value) as total_value 
FROM large_table 
GROUP BY category;

.print ''
.print 'Execution Stats:'
SELECT execution_mode, query_duration_ms, num_workers_used, num_tasks_generated 
FROM duckherder_get_query_execution_stats() 
ORDER BY execution_start_time DESC LIMIT 1;
