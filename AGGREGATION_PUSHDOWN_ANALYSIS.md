# Aggregation Pushdown Analysis & Fix Plan

## Problem Discovered

When executing aggregation queries like:
```sql
SELECT category, SUM(value) FROM test GROUP BY category
```

**Workers return RAW ROWS, not aggregated results:**
```
Worker output: [(0, 'A', 0), (1, 'B', 100), (2, 'A', 200), ...]
Expected: [('A', 500), ('B', 1000)]
```

**This causes incorrect results** because aggregation happens only on the coordinator after fetching ALL rows.

---

## Root Cause

### Architecture Flow

```
User Query: SELECT category, SUM(value) FROM test GROUP BY category
    ↓
Client-side Distributed Table Scan Function intercepts
    ↓
Sends ScanTableRequest(table_name="test")
    ↓
Server HandleScanTable() - distributed_flight_server.cpp:389
    string sql = StringUtil::Format("SELECT * FROM %s", req.table_name());
    ↓
Workers execute: SELECT * FROM test WHERE rowid BETWEEN X AND Y
    ↓
Workers return RAW ROWS
    ↓
Client-side aggregation (defeating distributed execution!)
```

### The Bug Location

**File**: `src/server/driver/distributed_flight_server.cpp`
**Line**: 389

```cpp
arrow::Status DistributedFlightServer::HandleScanTable(...) {
    string sql = StringUtil::Format("SELECT * FROM %s", req.table_name());
    //                              ^^^^^^^^^^^^^^^^^^
    //                              HARDCODED SELECT *!
```

---

## Why This Happens

The system has **two execution paths**:

### Path 1: ScanTable (Current - BROKEN for aggregations)
- Used by: Client-side `DistributedTableScanFunction`
- Request type: `ScanTableRequest` (only contains table name)
- Server generates: `SELECT * FROM table` (hardcoded)
- **Problem**: Aggregations are stripped out!

### Path 2: ExecutePartition (Works correctly!)
- Used by: Distributed executor for multi-worker queries
- Request type: `ExecutePartitionRequest` (contains full SQL)
- Server executes: Full SQL with aggregations
- **This already works!** But it's not used for single distributed table queries

---

## The Solution

### Option 1: Add SQL Field to ScanTableRequest (Protocol Change)

**Pros**:
- Clean separation of concerns
- Minimal code changes

**Cons**:
- Requires protobuf changes
- Backward compatibility concerns

**Implementation**:
1. Add `optional string custom_sql` to `ScanTableRequest` proto
2. In `HandleScanTable`, use `custom_sql` if provided, else default to `SELECT *`
3. In client-side table scan function, set `custom_sql` from the query

### Option 2: Route Aggregations Through ExecutePartition (Recommended)

**Pros**:
- No protocol changes needed
- Reuses existing distributed executor infrastructure
- Already supports full SQL with partitioning

**Cons**:
- More complex client-side logic
- Need to detect which queries have aggregations

**Implementation**:
1. In client-side table scan function, detect if query has aggregations
2. If yes, route through `ExecutePartition` instead of `ScanTable`
3. Use distributed executor's existing aggregation handling

### Option 3: Make ScanTable Smarter (Quick Fix)

**Pros**:
- Minimal changes
- Works immediately

**Cons**:
- Hacky - mixing concerns
- ScanTable becomes a misnomer

**Implementation**:
In `HandleScanTable`, instead of hardcoded `SELECT *`:

```cpp
// If limit/offset suggest this is part of a larger query, try full execution
string sql = req.table_name();  // Assume table_name might contain full SQL
if (sql.find("SELECT") == string::npos) {
    // Just a table name, use SELECT *
    sql = StringUtil::Format("SELECT * FROM %s", sql);
}
// Now sql could be full query with aggregations
```

---

## Recommended Approach: Option 2

Use **ExecutePartition path** for queries with aggregations.

### Step 1: Detect Aggregation Queries (Client-Side)

In `DistributedTableScanFunction::Bind()` or during query planning:

```cpp
// Check if query has aggregations/GROUP BY
bool has_aggregations = /* detect from logical plan */;

if (has_aggregations && worker_count > 1) {
    // Route through ExecutePartition (distributed executor)
    return UseDistributedExecutor(sql, workers);
} else {
    // Use simple ScanTable for basic scans
    return UseScanTable(table_name);
}
```

### Step 2: Pass Full SQL to Distributed Executor

The distributed executor already does this correctly!

```cpp
// In ExecuteDistributed:
auto tasks = ExtractPipelineTasks(logical_plan, sql, workers);
// sql contains FULL query including aggregations
```

### Step 3: Workers Execute Partial Aggregations

Workers already support this via `ExecutePartitionRequest`:

```sql
-- Worker 0 executes:
SELECT category, SUM(value) 
FROM test 
WHERE rowid BETWEEN 0 AND 245759 
GROUP BY category

-- Returns: [('A', partial_sum_A), ('B', partial_sum_B)]
```

### Step 4: Coordinator Re-Aggregates

Step 4b already implements this!

```cpp
// In CollectAndMergeResults with GROUP_BY_MERGE strategy:
// 1. Collect partial results from workers
// 2. Create temp table
// 3. Execute: SELECT category, SUM(sum_value) FROM temp GROUP BY category
```

---

## Current Status

### What Works ✅
- Row group-based partitioning
- Multi-worker task distribution
- Step 4b smart merging infrastructure (ready but unused)
- `ExecutePartition` path supports full SQL

### What's Broken ❌
- Aggregation queries use `ScanTable` path
- `ScanTable` hardcoded to `SELECT *`
- Aggregations happen client-side after fetching ALL rows
- Step 4b never triggered because workers return raw rows

---

## Implementation Plan

### Phase 1: Quick Verification
1. Manually test `ExecutePartition` with aggregation SQL
2. Confirm workers can execute `GROUP BY` queries
3. Confirm Step 4b merging works

### Phase 2: Route Aggregations Correctly
1. Add logic to detect aggregation queries
2. Route them through distributed executor
3. Ensure `base_sql` passed to `ExtractPipelineTasks` includes aggregations

### Phase 3: Handle Edge Cases
1. Mix of aggregated and non-aggregated columns
2. Nested aggregations
3. HAVING clauses
4. DISTINCT with aggregations

---

## Test Case

```sql
CREATE TABLE test (id INTEGER, category VARCHAR, value INTEGER);
INSERT INTO test SELECT i, CASE (i%2) WHEN 0 THEN 'A' ELSE 'B' END, i*100 
FROM range(0, 500000) t(i);

SELECT category, COUNT(*) as cnt, SUM(value) as sum_val 
FROM test 
GROUP BY category;
```

### Expected Execution:
```
Worker 0: SELECT category, SUM(value) FROM test WHERE rowid BETWEEN 0-245759 GROUP BY category
  → Returns: [('A', 1509847168000), ('B', 1509969024000)]

Worker 1: SELECT category, SUM(value) FROM test WHERE rowid BETWEEN 245760-491519 GROUP BY category
  → Returns: [('A', 1510990976000), ('B', 1511112832000)]

Coordinator: SELECT category, SUM(partial_sum) FROM results GROUP BY category
  → Final: [('A', total_A), ('B', total_B)]
```

### Current (Broken) Execution:
```
Worker 0: SELECT * FROM test WHERE rowid BETWEEN 0-245759
  → Returns: 245760 raw rows

Worker 1: SELECT * FROM test WHERE rowid BETWEEN 245760-491519
  → Returns: 245760 raw rows

Coordinator: Fetches 500K rows, does aggregation locally ❌
```

---

## Next Steps

1. **Immediate**: Verify the fix works by modifying `HandleScanTable` temporarily
2. **Short-term**: Implement Option 2 (route through ExecutePartition)
3. **Long-term**: Consider protocol changes for cleaner separation

---

## Key Insight

**The infrastructure for distributed aggregations already exists (Step 4b)!**

We just need to ensure:
1. Aggregation SQL reaches the workers (not stripped to `SELECT *`)
2. Queries are routed through the correct execution path

The fix is about **routing and SQL preservation**, not building new aggregation logic.

