# True Natural Parallelism Implementation Plan

## Goal
Map DuckDB's internal parallel execution tasks directly to distributed worker nodes, so that if DuckDB decides to split a query into 8 parallel tasks, we can execute those 8 tasks across our N workers.

## Current State vs Target State

### Current (Statistics-Informed Manual Partitioning)
```
Coordinator:
1. Query physical plan for cardinality (10,000 rows)
2. Calculate rows_per_partition = 10,000 / 4 = 2,500
3. Generate manual SQL: "WHERE rowid BETWEEN X AND Y"
4. Send to workers
5. Workers execute independently
6. Coordinator merges results
```

### Target (Natural DuckDB Parallelism)
```
Coordinator:
1. Build physical plan with pipelines
2. DuckDB creates N parallel tasks (e.g., 8 PipelineTasks)
3. Map these 8 tasks across 4 workers (2 tasks per worker)
4. Each task has its own LocalState (position in data)
5. Workers execute task-by-task, reading from shared GlobalState
6. Results flow through pipeline and merge via GlobalSinkState
```

## Key DuckDB Concepts

### 1. **Executor → MetaPipeline → Pipeline → PipelineTask**
```cpp
// Executor builds the pipeline tree from physical plan
Executor executor;
executor.Initialize(physical_plan);

// Creates MetaPipelines which contain Pipelines
// Each Pipeline has:
- PhysicalOperator *source     // Where data comes from (e.g., TableScan)
- PhysicalOperator *sink        // Where data goes (e.g., Aggregate, Result)
- vector<PhysicalOperator> operators  // Intermediate ops (Filter, Projection)

// Scheduling parallelism (pipeline.cpp:101-134)
bool Pipeline::ScheduleParallel(Event &event) {
    auto max_threads = source_state->MaxThreads();  // e.g., 8 for large table
    
    // Creates PipelineTasks
    for (idx_t i = 0; i < max_threads; i++) {
        tasks.push_back(make_uniq<PipelineTask>(*this, event));
    }
}
```

### 2. **GlobalSourceState & LocalSourceState**
```cpp
// GlobalSourceState: Shared state coordinating parallel access
class TableScanGlobalState {
    ParallelTableScanState state;  // Tracks: current_row_group, vector_index, etc.
    mutex lock;                    // Synchronizes NextParallelScan()
    idx_t max_threads;            // How many parallel tasks
};

// LocalSourceState: Per-task state (one per PipelineTask)
class TableScanLocalState {
    TableScanState scan_state;     // My position in the table
    DataChunk buffer;              // My local buffer
};

// Key method: NextParallelScan (data_table.cpp:290)
bool DataTable::NextParallelScan(ClientContext &context, 
                                 ParallelTableScanState &state,  // Global
                                 TableScanState &scan_state) {    // Local
    // Atomically assigns next chunk of work to this task
    // Returns false when all work is done
}
```

### 3. **GlobalSinkState & LocalSinkState**
```cpp
// GlobalSinkState: Where final results accumulate
class HashAggregateGlobalState {
    GroupedAggregateHashTable ht;  // Final aggregation results
    mutex lock;                     // For Combine()
};

// LocalSinkState: Per-task aggregation buffer
class HashAggregateLocalState {
    GroupedAggregateHashTable ht;  // My local aggregation
    
    void Sink(DataChunk &input) {
        // Aggregate locally
        ht.AddChunk(input);
    }
    
    void Combine(GlobalState &global) {
        // Merge my results into global
        global.ht.Combine(ht);
    }
};
```

## Implementation Steps

### **Step 1: Serialize Pipeline Tasks (Not Just Plans)**

**Goal**: Instead of serializing just the logical plan, serialize the entire pipeline structure with task information.

**What to Send to Workers**:
```cpp
struct DistributedTask {
    string serialized_pipeline;        // The Pipeline (source, sink, operators)
    idx_t task_id;                     // Task number (0-7 if 8 tasks)
    idx_t total_tasks;                 // Total parallel tasks (8)
    string serialized_global_state;    // Initial GlobalSourceState
};
```

**Implementation**:
```cpp
// In distributed_executor.cpp
vector<DistributedTask> PrepareDistributedTasks(PhysicalOperator &plan) {
    // 1. Build executor and pipelines
    Executor executor(context);
    executor.Initialize(plan);
    
    // 2. Get the root pipeline
    auto &pipelines = executor.GetPipelines();
    auto &pipeline = pipelines[0];  // Focus on main scan pipeline first
    
    // 3. Query how many parallel tasks DuckDB would create
    auto global_source = pipeline.source->GetGlobalSourceState(context);
    idx_t max_threads = global_source->MaxThreads();
    
    // 4. Create a DistributedTask for each parallel task
    vector<DistributedTask> tasks;
    for (idx_t i = 0; i < max_threads; i++) {
        DistributedTask task;
        task.task_id = i;
        task.total_tasks = max_threads;
        task.serialized_pipeline = SerializePipeline(pipeline);
        task.serialized_global_state = SerializeGlobalState(global_source);
        tasks.push_back(task);
    }
    
    return tasks;
}
```

### **Step 2: Distribute Tasks Across Workers**

**Goal**: Map M tasks to N workers (M >= N).

**Strategy**:
```cpp
// Round-robin or work-stealing scheduling
void DistributeTasks(vector<DistributedTask> &tasks, vector<Worker> &workers) {
    // Example: 8 tasks, 4 workers → 2 tasks per worker
    for (idx_t i = 0; i < tasks.size(); i++) {
        idx_t worker_id = i % workers.size();
        workers[worker_id].AddTask(tasks[i]);
    }
}
```

**Alternative (Work Stealing)**:
- Workers pull tasks from a shared queue
- More dynamic load balancing
- Requires coordinator to manage task queue

### **Step 3: Worker-Side Pipeline Execution**

**Goal**: Workers reconstruct the pipeline and execute their assigned task(s).

**Implementation**:
```cpp
// In worker_node.cpp
void WorkerNode::ExecutePipelineTask(DistributedTask &task) {
    // 1. Deserialize pipeline
    Pipeline pipeline = DeserializePipeline(task.serialized_pipeline);
    
    // 2. Recreate GlobalSourceState
    auto global_source = DeserializeGlobalState(task.serialized_global_state);
    
    // 3. Create LocalSourceState for THIS task
    auto local_source = pipeline.source->GetLocalSourceState(context, *global_source);
    
    // 4. Execute the pipeline task
    PipelineExecutor executor(context, pipeline);
    
    DataChunk intermediate_chunk;
    while (true) {
        // Pull data using the local state
        auto result = pipeline.source->GetData(context, intermediate_chunk, 
                                               *global_source, *local_source);
        
        if (result == SourceResultType::FINISHED) {
            break;
        }
        
        // Execute intermediate operators (filters, projections)
        for (auto &op : pipeline.operators) {
            op.Execute(context, intermediate_chunk);
        }
        
        // Push to sink (local aggregation, etc.)
        pipeline.sink->Sink(context, *local_sink_state, intermediate_chunk);
    }
    
    // 5. Return local sink state to coordinator for merging
    return SerializeLocalSinkState(local_sink_state);
}
```

### **Step 4: Coordinator-Side Result Merging (Combine)**

**Goal**: Merge LocalSinkStates from all workers into final GlobalSinkState.

**Implementation**:
```cpp
// In distributed_executor.cpp
unique_ptr<QueryResult> MergeDistributedResults(
    vector<SerializedLocalSinkState> &worker_results,
    PhysicalOperator &sink) {
    
    // 1. Create GlobalSinkState
    auto global_sink = sink.GetGlobalSinkState(context);
    
    // 2. Deserialize and combine each worker's LocalSinkState
    for (auto &serialized : worker_results) {
        auto local_sink = DeserializeLocalSinkState(serialized);
        
        // Call the operator's Combine method
        // This is DuckDB's natural merge operation
        sink.Combine(context, *global_sink, *local_sink);
    }
    
    // 3. Finalize the sink (e.g., compute final aggregates)
    sink.Finalize(context, *global_sink);
    
    // 4. Extract final results
    return sink.GetResult(context, *global_sink);
}
```

### **Step 5: Handle Multi-Pipeline Queries**

**Challenge**: Complex queries have multiple pipelines (e.g., build hash table → probe hash table).

**Solution**:
```cpp
// Process pipelines in dependency order
void ExecuteMultiPipelineQuery(PhysicalOperator &plan) {
    Executor executor(context);
    executor.Initialize(plan);
    
    // Get all pipelines in execution order
    auto &pipelines = executor.GetPipelines();
    
    for (auto &pipeline : pipelines) {
        // Check dependencies
        if (pipeline.RequiresPreviousPipeline()) {
            // Wait for previous pipeline's GlobalSinkState
            auto dependency = pipeline.GetDependency();
            // Previous pipeline's result becomes this pipeline's source
        }
        
        // Execute this pipeline across workers
        auto tasks = PrepareDistributedTasks(*pipeline);
        auto results = DistributeAndExecute(tasks);
        auto merged = MergeResults(results, *pipeline->sink);
        
        // Store for dependent pipelines
        pipeline_results[pipeline.id] = merged;
    }
}
```

### **Step 6: Serialize/Deserialize Pipeline State**

**Challenge**: Transfer GlobalSourceState/LocalSinkState across network.

**Approaches**:

**Option A: Use DuckDB's Native Serialization**:
```cpp
// DuckDB has built-in serialization for many types
string SerializeGlobalState(GlobalSourceState &state) {
    MemoryStream stream;
    BinarySerializer serializer(stream);
    serializer.Begin();
    state.Serialize(serializer);  // If available
    serializer.End();
    return stream.GetData();
}
```

**Option B: Custom Serialization**:
```cpp
// For types without built-in serialization
struct SerializedTableScanState {
    idx_t current_row_group;
    idx_t vector_index;
    idx_t max_row;
    idx_t batch_index;
    
    string Serialize() {
        // Convert to JSON or protobuf
    }
};
```

**Option C: Stateless Approach (Simpler)**:
```cpp
// Don't serialize actual state - just task metadata
struct TaskAssignment {
    idx_t row_group_start;
    idx_t row_group_end;
    idx_t vector_offset;
};

// Each worker independently constructs its position
void InitializeTaskState(TaskAssignment &assignment) {
    scan_state.row_group = row_groups[assignment.row_group_start];
    scan_state.vector_index = assignment.vector_offset;
}
```

## Concrete Example: Aggregation Query

### Query:
```sql
SELECT category, SUM(amount) 
FROM large_table 
GROUP BY category;
```

### DuckDB's Natural Execution:
```
Pipeline 1: Scan → Aggregate (Build Hash Table)
├─ Source: PhysicalTableScan
├─ Sink: PhysicalHashAggregate
└─ Parallel Tasks: 8 (based on table size)

Task 0: Scans row_groups 0-1   → Local HT with partial sums
Task 1: Scans row_groups 2-3   → Local HT with partial sums
Task 2: Scans row_groups 4-5   → Local HT with partial sums
...
Task 7: Scans row_groups 14-15 → Local HT with partial sums

Combine Phase: Merge all 8 local HTs into Global HT
Finalize Phase: Extract final results from Global HT
```

### Distributed Execution:
```
Coordinator:
1. Builds Pipeline 1
2. Discovers 8 parallel tasks needed
3. Distributes to 4 workers (2 tasks each)

Worker 0: Executes Tasks 0 & 1 → Returns LocalSinkState (partial HT)
Worker 1: Executes Tasks 2 & 3 → Returns LocalSinkState (partial HT)
Worker 2: Executes Tasks 4 & 5 → Returns LocalSinkState (partial HT)
Worker 3: Executes Tasks 6 & 7 → Returns LocalSinkState (partial HT)

Coordinator:
4. Receives 4 LocalSinkStates
5. Calls Combine() 4 times to merge into GlobalSinkState
6. Calls Finalize() to get final results
7. Returns to client
```

## Challenges & Solutions

### Challenge 1: Stateful Sources
**Problem**: Some sources (like CSV readers) maintain file position state.

**Solution**: 
- Use DuckDB's `ParallelTableScanState` which is designed for this
- Each task calls `NextParallelScan()` to atomically get its work chunk
- For distributed: Workers can pull task assignments from coordinator dynamically

### Challenge 2: Cross-Worker Dependencies
**Problem**: Hash joins require building hash table before probing.

**Solution**:
- Execute pipelines in order
- Pipeline 1 (build): All workers contribute to building shared hash table
- Coordinator merges into single hash table
- Pipeline 2 (probe): Distribute hash table to all workers for probing

### Challenge 3: Network Overhead
**Problem**: Serializing/deserializing state is expensive.

**Solution**:
- Start with simple operators (scans, filters, aggregates)
- Use Apache Arrow for zero-copy data transfer
- Batch multiple tasks per worker to amortize setup cost
- Consider shared-memory approach for single-machine multi-process

### Challenge 4: Dynamic Task Allocation
**Problem**: Some tasks finish faster than others (data skew).

**Solution**:
- Work-stealing: Workers request new tasks when idle
- Dynamic task queue on coordinator
- Adaptive task granularity

## Recommended Implementation Order

### Phase 1: Simple Scan + Aggregate (Week 1-2)
- ✅ Already done: Statistics extraction
- ✅ Already done: Basic distributed execution
- 🔲 Task: Serialize simple pipelines (scan → aggregate)
- 🔲 Task: Execute pipeline tasks on workers
- 🔲 Task: Implement Combine() for aggregates

### Phase 2: Filters & Projections (Week 3)
- 🔲 Handle intermediate operators in pipeline
- 🔲 Test with complex WHERE clauses
- 🔲 Test with computed columns

### Phase 3: Multi-Pipeline Queries (Week 4-5)
- 🔲 Handle pipeline dependencies
- 🔲 Implement hash join distribution
- 🔲 Handle subqueries

### Phase 4: Advanced Features (Week 6+)
- 🔲 Work-stealing scheduler
- 🔲 Adaptive partitioning
- 🔲 Fault tolerance
- 🔲 Cost-based worker assignment

## Proof of Concept Code Skeleton

```cpp
// Step 1: Extract pipeline tasks
auto tasks = ExtractPipelineTasks(physical_plan);
// → Returns: [{task_id: 0, row_groups: [0,1]}, {task_id: 1, row_groups: [2,3]}, ...]

// Step 2: Distribute to workers
for (idx_t i = 0; i < tasks.size(); i++) {
    idx_t worker_id = i % workers.size();
    futures[i] = workers[worker_id].ExecuteTaskAsync(tasks[i]);
}

// Step 3: Collect local sink states
vector<LocalSinkState> local_results;
for (auto &future : futures) {
    local_results.push_back(future.get());
}

// Step 4: Combine into global state
auto global_sink = CreateGlobalSinkState(physical_plan.sink);
for (auto &local : local_results) {
    physical_plan.sink->Combine(context, *global_sink, local);
}

// Step 5: Finalize and return results
physical_plan.sink->Finalize(context, *global_sink);
return ExtractResults(*global_sink);
```

## Success Metrics

1. ✅ **Correctness**: Results match single-node DuckDB exactly
2. ✅ **Scalability**: Near-linear speedup with worker count
3. ✅ **Efficiency**: Minimal coordinator overhead
4. ✅ **Generality**: Works for most query types (scans, filters, joins, aggregates)
5. ✅ **Observability**: Can trace which tasks executed where

## Next Immediate Actions

1. **Read Existing Pipeline Code**: Study `pipeline.cpp`, `pipeline_executor.cpp`, `executor.cpp`
2. **Prototype Task Extraction**: Write `ExtractPipelineTasks()` for simple table scan
3. **Test Locally**: Execute multiple PipelineTasks in same process, verify correctness
4. **Design Serialization**: Decide on state transfer format
5. **Implement Worker Execution**: Modify `worker_node.cpp` to execute pipeline tasks
6. **Implement Coordinator Combine**: Add `Combine()` logic in `distributed_executor.cpp`

This is ambitious but achievable! The key insight is that DuckDB's architecture *already* supports exactly what we need - we just need to map its thread-based parallelism to node-based parallelism.

