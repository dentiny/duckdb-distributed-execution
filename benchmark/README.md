# DuckDB Distributed Execution Benchmarks

SQL-based benchmarks comparing raw DuckDB vs distributed execution performance.

## Benchmark Files

Each `.sql` file contains two tests run sequentially:
- **Test A**: Baseline (No distribution)
- **Test B**: Distributed (4 workers)

1. **`small_table.sql`** - 1,000 rows
   - Expected: DELEGATED mode (single task)

2. **`medium_table.sql`** - 200,000 rows
   - Expected: NATURAL_PARTITION (range-based)

3. **`large_table.sql`** - 500,000 rows (~4 row groups)
   - Expected: ROW_GROUP_PARTITION

## Running Benchmarks

```bash
# Run single benchmark
./build/reldebug/duckdb :memory: < small_table.sql
```
