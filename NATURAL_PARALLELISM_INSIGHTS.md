# Natural Parallelism Insights

## Key Discovery: DuckDB's Parallel Execution Model

### How DuckDB Parallelizes Queries

DuckDB uses a sophisticated work distribution system:

1. **`GlobalSourceState::MaxThreads()`** - Returns how many parallel tasks are optimal
   - Table scans: Based on row groups (e.g., 1000 rows → 8 row groups → 8 threads)
   - Hash aggregates: Based on hash partitions  
   - Joins: Based on data blocks

2. **`NextParallelScan()` / `AssignTask()`** - Distributes actual work units
   - Each thread calls this to get its next partition
   - Work assignment is **dynamic** (not predetermined)
   - Thread gets "row group 3" or "hash partition 7", etc.

### Example: Table Scan Parallelization

```cpp
// RowGroupCollection::NextParallelScan
bool NextParallelScan(...) {
    lock_guard<mutex> l(state.lock);
    
    // Atomically assign next row group to this thread
    row_group = state.current_row_group;
    state.current_row_group = GetNextSegment(row_group);
    
    return true; // Thread got work!
}
```

## 🎯 Implication for Distributed Execution

### The Challenge

DuckDB's parallelism is **dynamic and stateful**:
- Work is assigned on-demand via shared `GlobalSourceState`
- Threads compete for work units using locks
- This works great for threads in the same process
- But **not directly applicable** to distributed nodes

### Why We Can't Directly Extract Partitions

```
❌ DOESN'T WORK:
Coordinator: "Extract all row group boundaries"
             → Serialize to workers
Problem: Row groups are assigned dynamically, not predetermined!
```

###  Three Approaches (Ranked by Complexity)

#### Approach 1: Simple Static Partitioning (CURRENT)
```
✅ What we have now:
- Use MaxThreads() to understand natural parallelism
- Create N static partitions: WHERE (rowid % N) = worker_id
- Send plan to workers

Pros: Simple, works for all query types
Cons: Less optimal than DuckDB's native partitioning
```

#### Approach 2: Partition-Aware Distribution (STEP 2 - NEXT)
```
🎯 What we'll implement:
- Interrogate the physical plan for partition hints
- For table scans: Get row group count and boundaries
- Create explicit partition predicates per worker
- Send partitioned plan to each worker

Pros: More aligned with DuckDB's decisions
Cons: Requires plan analysis, only works for some operators
```

#### Approach 3: Remote GlobalState (FUTURE - COMPLEX)
```
🔮 Future possibility:
- Coordinator maintains GlobalSourceState
- Workers RPC back to get work: "GetNextPartition()"
- Coordinator assigns work dynamically

Pros: Closest to DuckDB's native model
Cons: Very complex, high latency, chatty protocol
```

## 📋 Step 2 Implementation Plan

### Goal
Make partitioning more intelligent by analyzing the physical plan.

### Concrete Actions

1. **Analyze Physical Plan Structure**
   ```cpp
   // For table scans, extract table info
   if (physical_plan.type == PhysicalOperatorType::TABLE_SCAN) {
       auto &table_scan = (PhysicalTableScan&)physical_plan;
       // Get row group information
       // Create per-worker predicates based on row groups
   }
   ```

2. **Create Per-Worker Partition Info**
   ```cpp
   struct WorkerPartitionInfo {
       idx_t worker_id;
       idx_t total_workers;
       idx_t row_group_start;  // For table scans
       idx_t row_group_end;
   };
   ```

3. **Augment Serialized Plan**
   - Include partition metadata
   - Workers reconstruct plan with partition constraints

### What We're NOT Doing (For Now)

- ❌ Trying to serialize `GlobalSourceState`
- ❌ Implementing dynamic work stealing
- ❌ Supporting all operator types' native partitioning

### Success Criteria

- [x] Step 1: Log `MaxThreads()` from physical plan ✅
- [ ] Step 2: Analyze plan structure and create smarter partitions
- [ ] Step 3: Test that partitions cover all data exactly once
- [ ] Step 4: Compare performance vs manual rowid partitioning

## 🔬 Testing Natural Parallelism

Run the test to see DuckDB's decisions:

```bash
make test_reldebug 2>&1 | grep "📊"
```

Expected output:
```
📊 [PARALLELISM] DuckDB's natural parallelism decision:
📊 [PARALLELISM]    - Estimated thread count: 4
📊 [PARALLELISM]    - Physical plan type: TABLE_SCAN
```

This tells us what DuckDB *would* do if running locally.
Our job: Map those 4 parallel tasks to 4 distributed workers.

