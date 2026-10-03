-- Startup SQL that simulates S3 latency; pass it as DUCKHERDER_STARTUP_SQL. Requires a build with latency_injection_fs.
SET GLOBAL latency_inject_fs_read_base_mean_ms = 30;
SET GLOBAL latency_inject_fs_read_base_stddev = 15;
SET GLOBAL latency_inject_fs_read_bytes_per_ms = 88000;
SET GLOBAL latency_inject_fs_stat_mean_ms = 20;
SET GLOBAL latency_inject_fs_stat_stddev = 10;
SET GLOBAL latency_inject_fs_list_mean_ms = 40;
SET GLOBAL latency_inject_fs_list_stddev = 20;
SELECT latency_inject_fs_wrap('SlateDBFileSystem');
