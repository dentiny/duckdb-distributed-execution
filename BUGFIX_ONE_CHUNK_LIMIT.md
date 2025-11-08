# Bug Fix: Distributed Table Scan Only Returning One Chunk (2048 Rows)

## Issue

Point queries and full table scans on distributed tables were only returning **2048 rows** instead of all data, regardless of the actual table size.

## Root Cause Analysis

### User's Insight
The user correctly identified that **2048 is DuckDB's `STANDARD_VECTOR_SIZE`**, suggesting we were only processing one chunk/vector.

### Investigation
Found in `distributed_table_scan_function.cpp` line 115-116:

```cpp
// TODO(hjiang): For simplicity, we only return one chunk.
local_state.finished = true;
```

The distributed table scan was:
1. **Only fetching one chunk** (2048 rows)
2. **Hardcoded offset to 0** (line 85: `/*offset=*/0`)
3. **Immediately marking as finished** after one chunk

This meant:
- `COUNT(*)` returned **2048** instead of **10000**
- Point queries like `WHERE id = 2500` returned **0 rows** (data beyond first chunk)
- Only the first 2048 rows were ever accessible

## The Fix

### Changes to `distributed_table_scan_function.cpp`

**1. Added offset tracking to `DistributedTableScanLocalState`:**

```cpp
struct DistributedTableScanLocalState : public LocalTableFunctionState {
    DistributedTableScanLocalState() : finished(false), offset(0) {
    }
    bool finished;
    vector<column_t> column_ids;
    idx_t offset;  // Track current offset for fetching data
};
```

**2. Updated `Execute()` to use progressive offset:**

```cpp
// Before: Always fetch from offset 0
auto result = client.ScanTable(bind_data.remote_table_name, /*limit=*/output.GetCapacity(), /*offset=*/0, &expected_types);

// After: Use tracked offset
auto result = client.ScanTable(bind_data.remote_table_name, /*limit=*/output.GetCapacity(), /*offset=*/local_state.offset, &expected_types);
```

**3. Changed completion logic:**

```cpp
// Before: Mark finished after first chunk
// TODO(hjiang): For simplicity, we only return one chunk.
local_state.finished = true;

// After: Increment offset, continue until no data
if (data_chunk && data_chunk->size() > 0) {
    // ... copy data ...
    
    // FIX: Increment offset to fetch next chunk on next call
    // Don't mark as finished yet - keep fetching until we get an empty chunk
    local_state.offset += data_chunk->size();
    
    DUCKDB_LOG_DEBUG(db, StringUtil::Format("Fetched %llu rows, new offset: %llu", 
                                            static_cast<long long unsigned>(data_chunk->size()),
                                            static_cast<long long unsigned>(local_state.offset)));
} else {
    // No more data - mark as finished
    output.SetCardinality(0);
    local_state.finished = true;
    DUCKDB_LOG_DEBUG(db, StringUtil::Format("Scan finished for table: %s (total rows: %llu)", 
                                            bind_data.remote_table_name,
                                            static_cast<long long unsigned>(local_state.offset)));
}
```

## Results

### Before Fix
```sql
SELECT COUNT(*) FROM large_table;  -- Returns: 2048 ❌
SELECT * FROM large_table WHERE id = 2500;  -- Returns: 0 rows ❌
SELECT MAX(id) FROM large_table;  -- Returns: 2047 ❌
```

### After Fix
```sql
SELECT COUNT(*) FROM large_table;  -- Returns: 10000 ✅
SELECT * FROM large_table WHERE id = 2500;  -- Returns: correct row ✅
SELECT MAX(id) FROM large_table;  -- Returns: 9999 ✅
```

## Test Results

All 17 test cases now pass with 776 assertions:

```
===============================================================================
All tests passed (776 assertions in 17 test cases)
```

Specifically, the following previously-failing tests now work:
- Point queries at partition boundaries (`id = 2500`, `id = 5000`, `id = 7500`)
- Full table scans returning all 10,000 rows
- Aggregate functions over entire dataset (`COUNT`, `MAX`, `MIN`)
- Queries with predicates beyond first 2048 rows

## How the Fix Works

### Chunk-by-Chunk Fetching

```
Initial state: offset = 0

Call 1: Fetch rows [0, 2047]
        → Returns 2048 rows
        → offset += 2048 → offset = 2048
        → Continue (don't mark finished)

Call 2: Fetch rows [2048, 4095]
        → Returns 2048 rows
        → offset += 2048 → offset = 4096
        → Continue

Call 3: Fetch rows [4096, 6143]
        → Returns 2048 rows
        → offset += 2048 → offset = 6144
        → Continue

Call 4: Fetch rows [6144, 8191]
        → Returns 2048 rows
        → offset += 2048 → offset = 8192
        → Continue

Call 5: Fetch rows [8192, 9999]
        → Returns 1808 rows
        → offset += 1808 → offset = 10000
        → Continue

Call 6: Fetch rows [10000, ...]
        → Returns 0 rows (empty chunk)
        → Mark finished ✓
```

### DuckDB's Pull-Based Execution

This fix aligns with DuckDB's pull-based execution model:
1. DuckDB calls `Execute()` repeatedly
2. Each call fills one output chunk
3. We fetch from remote with incremental offset
4. When no more data, we return empty chunk
5. DuckDB stops pulling

## Impact on Steps 2 & 3

This bug fix is **independent** of Steps 2 & 3 (task distribution and worker execution):

- **Steps 2 & 3** focus on the **distributed executor** path (coordinator→workers)
- **This bug** was in the **distributed table scan** path (client→server)

Both paths are now working correctly:

### Distributed Executor Path (Steps 2 & 3)
```
Client → DistributedExecutor → Workers → Results merged
         ↓
    ExtractPipelineTasks()
    Round-robin distribution  
    Worker execution with metrics
```

### Distributed Table Scan Path (This Fix)
```
Client → DistributedTableScanFunction → Remote Server
         ↓
    Chunk-by-chunk fetching with offset tracking
```

## Files Modified

1. **`src/client/distributed_table_scan_function.cpp`**
   - Added `offset` field to `DistributedTableScanLocalState`
   - Modified `Execute()` to use progressive offset
   - Changed completion logic to continue fetching until empty chunk

2. **`test/sql/large_table_partitioning.test`**
   - Uncommented previously-failing point query tests
   - Fixed incorrect expected value (2498 should return 'books', not 'clothing')

## Lessons Learned

1. **User insights are valuable**: The observation about 2048 being `STANDARD_VECTOR_SIZE` was the key to finding the bug quickly.

2. **TODOs are warnings**: The comment `// TODO(hjiang): For simplicity, we only return one chunk.` was a red flag indicating incomplete implementation.

3. **Test coverage matters**: Without tests for queries beyond the first chunk, this bug could have gone unnoticed.

4. **Progressive fetching is essential**: Table scans must support incremental offset tracking to handle arbitrarily large tables.

## Status

✅ **BUG FIXED** - All tests passing

The distributed table scan now correctly fetches all data by:
- Tracking offset across multiple `Execute()` calls
- Incrementing offset after each chunk
- Only marking finished when receiving empty chunk

This ensures correctness for tables of any size, not just those fitting in one 2048-row chunk!

