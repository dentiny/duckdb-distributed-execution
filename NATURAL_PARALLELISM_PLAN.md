# Plan: Implement Natural DuckDB Parallelism for Distributed Execution

## Goal
Map DuckDB's natural parallel execution (PipelineTasks) to distributed workers, instead of manually partitioning by rowid.

## Current Architecture (Manual Partitioning) ❌

```
Coordinator:
  SELECT * FROM table
  ↓
  Manually inject partition predicates:
    Worker 0: WHERE (rowid % 4) = 0
    Worker 1: WHERE (rowid % 4) = 1
    Worker 2: WHERE (rowid % 4) = 2
    Worker 3: WHERE (rowid % 4) = 3
```

**Problem:** Ignores DuckDB's intelligent parallelization decisions.

## Target Architecture (Natural Parallelism) ✅

```
Coordinator:
  SELECT * FROM table
  ↓
  Build physical plan
  ↓
  DuckDB determines: "This pipeline can run with 4 parallel tasks"
  ↓
  Create 4 PipelineTasks
  ↓
  Send PipelineTask 0 → Worker 0
  Send PipelineTask 1 → Worker 1
  Send PipelineTask 2 → Worker 2
  Send PipelineTask 3 → Worker 3
  ↓
  Each worker executes its PipelineTask
  ↓
  Coordinator collects and merges results
```

## Key DuckDB Components

### 1. Pipeline (`duckdb/src/parallel/pipeline.cpp`)

**Line 101-135: `Pipeline::ScheduleParallel()`**
- Determines if pipeline can be parallelized
- Calculates `max_threads` from source's MaxThreads()
- Calls `LaunchScanTasks(event, max_threads)`

**Line 175-189: `Pipeline::LaunchScanTasks()`**
- Creates `max_threads` number of PipelineTasks
- Each task executes the same pipeline on different data

### 2. PipelineTask (`duckdb/src/parallel/pipeline_task.cpp`)

The actual unit of parallel execution that runs on a thread.

### 3. Executor (`duckdb/src/parallel/executor.cpp`)

**Line 381-422: `Executor::InitializeInternal()`**
- Builds pipelines from physical plan
- Schedules pipeline events

## Implementation Approach

### Phase 1: Intercept Pipeline Task Creation

**File to modify:** `src/server/driver/distributed_executor.cpp`

Instead of:
```cpp
// Current: Manual SQL partition
string partition_sql = CreatePartitionSQL(sql, partition_id, workers.size());
```

Do:
```cpp
// New: Extract PipelineTasks from DuckDB's executor
1. Build physical plan
2. Create Executor
3. Initialize pipelines (but don't execute)
4. Extract PipelineTasks that would be created
5. Serialize tasks
6. Send to workers
```

### Phase 2: Serialize PipelineTask State

Need to capture:
- Pipeline reference
- LocalSourceState (contains scan position/ranges)
- LocalSinkState (for collecting local results)
- Execution context

### Phase 3: Worker Executes PipelineTask

**File to modify:** `src/server/worker/worker_node.cpp`

```cpp
// Deserialize PipelineTask
// Create local execution context
// Execute task.Execute()
// Return results
```

### Phase 4: Coordinator Aggregates

Same as current - collect and merge results using GlobalSinkState semantics.

## Challenges & Solutions

### Challenge 1: PipelineTask Serialization

**Problem:** PipelineTasks contain complex state (LocalSourceState, LocalSinkState, execution context)

**Solution:** 
- Serialize the physical plan
- Serialize partition info (which data range to process)
- Worker reconstructs LocalSourceState/LocalSinkState from serialized info

### Challenge 2: Data Partitioning

**Problem:** How does each PipelineTask know which data to process?

**Solution:** 
- DuckDB's `LocalSourceState` already handles this!
- When you call `source->GetLocalSourceState()`, it automatically determines data ranges
- We just need to serialize this state

### Challenge 3: GlobalSourceState Coordination

**Problem:** `source_state->MaxThreads()` is computed locally

**Solution:**
- Let coordinator compute this
- Use number of workers as max_threads
- DuckDB will automatically partition data appropriately

## Detailed Implementation Plan

### Step 1: Modify DistributedExecutor

```cpp
unique_ptr<QueryResult> DistributedExecutor::ExecuteDistributed(const string &sql) {
    // 1. Build physical plan
    auto logical_plan = conn.ExtractPlan(sql);
    auto physical_plan = CreatePhysicalPlan(logical_plan);
    
    // 2. Build pipelines
    PipelineBuildState state;
    auto root_pipeline = make_shared_ptr<MetaPipeline>(*executor, state, nullptr);
    root_pipeline->Build(*physical_plan);
    root_pipeline->Ready();
    
    // 3. Get the source pipeline (the one we want to parallelize)
    auto &source_pipeline = /* extract scan pipeline */;
    
    // 4. Determine parallel tasks
    idx_t max_threads = workers.size(); // Use number of workers
    
    // 5. For each worker, create execution state
    for (idx_t i = 0; i < max_threads; i++) {
        // Create LocalSourceState for this worker
        auto local_source_state = source_pipeline.source->GetLocalSourceState(context, *source_state);
        
        // Serialize the state (partition info, scan ranges, etc.)
        auto serialized_state = SerializeLocalSourceState(local_source_state);
        
        // Send to worker
        SendToWorker(workers[i], physical_plan, serialized_state, i, max_threads);
    }
    
    // 6. Collect and merge results (same as current)
    return CollectAndMergeResults(...);
}
```

### Step 2: Modify WorkerNode

```cpp
arrow::Status WorkerNode::HandleExecutePartition(...) {
    // 1. Deserialize physical plan
    auto physical_plan = DeserializePhysicalPlan(req);
    
    // 2. Deserialize LocalSourceState
    auto local_source_state = DeserializeLocalSourceState(req);
    
    // 3. Create executor
    Executor executor(context);
    
    // 4. Execute with the deserialized state
    auto result = executor.ExecuteWithLocalState(physical_plan, local_source_state);
    
    // 5. Return results (same as current)
    return QueryResultToArrow(result, reader, row_count);
}
```

## Benefits of This Approach

1. ✅ **Respects DuckDB's Optimizer:** Uses DuckDB's parallelization decisions
2. ✅ **Handles Complex Queries:** Works with joins, aggregates, window functions
3. ✅ **Adaptive Parallelism:** Different queries get different parallelization strategies
4. ✅ **Better Performance:** DuckDB can use smarter partitioning than simple rowid modulo

## Example Scenarios

### Scenario 1: Table Scan
```sql
SELECT * FROM large_table WHERE value > 100;
```

DuckDB decides:
- Can parallelize across row groups
- Creates N tasks based on row group count
- Each task scans specific row groups

### Scenario 2: Join
```sql
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
```

DuckDB decides:
- Build phase: Single-threaded (or partitioned hash table)
- Probe phase: Parallel across partitions
- Creates tasks for probe phase parallelism

### Scenario 3: Aggregation
```sql
SELECT category, SUM(value) FROM table GROUP BY category;
```

DuckDB decides:
- Parallel scan and partial aggregation (LocalSinkState)
- Each task computes partial aggregates
- Final merge on coordinator (GlobalSinkState)

## Next Steps

1. **Research:** Study PipelineTask and LocalSourceState serialization
2. **Prototype:** Implement basic pipeline task extraction
3. **Test:** Verify with simple table scans first
4. **Extend:** Add support for more complex operators
5. **Optimize:** Fine-tune serialization and state management

## References

- `duckdb/src/parallel/pipeline.cpp` - Pipeline scheduling
- `duckdb/src/parallel/pipeline_task.cpp` - Task execution
- `duckdb/src/parallel/executor.cpp` - Executor initialization
- `duckdb/src/execution/physical_operator.cpp` - Operator states (LocalSourceState, LocalSinkState)

