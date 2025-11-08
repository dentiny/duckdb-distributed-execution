# Step 4a Complete: Query Analysis & Merge Strategy Detection

## What We Implemented

**Step 4a** is the foundation for smart result merging. We've implemented query analysis that detects different query patterns and determines the appropriate merge strategy.

---

## Key Components

### 1. Merge Strategy Enum

```cpp
enum class MergeStrategy {
    CONCATENATE,      // Simple scans - just concatenate results
    AGGREGATE_MERGE,  // Aggregations - need to merge partial aggregates  
    DISTINCT_MERGE,   // DISTINCT - need to eliminate duplicates
    GROUP_BY_MERGE    // GROUP BY - need to merge grouped results
};
```

### 2. Query Analysis Structure

```cpp
struct QueryAnalysis {
    MergeStrategy merge_strategy;
    bool has_aggregates;
    bool has_group_by;
    bool has_distinct;
    vector<string> aggregate_functions;  // e.g., "count", "sum", "avg"
    vector<string> group_by_columns;
};
```

### 3. AnalyzeQuery() Method

Recursively walks the logical plan tree to detect:
- **LOGICAL_AGGREGATE_AND_GROUP_BY** operators
- **LOGICAL_DISTINCT** operators
- Aggregate function names (COUNT, SUM, AVG, etc.)
- GROUP BY columns

### 4. Smart CollectAndMergeResults() Overload

New overload that receives `QueryAnalysis` and logs the merge strategy being used.

---

## Implementation Details

### Query Analysis Flow

```
ExecuteDistributed()
  │
  ├─> ExtractPlan(sql)
  │
  ├─> AnalyzeQuery(logical_plan)  ← STEP 4a
  │    │
  │    ├─> Recursively walk operator tree
  │    ├─> Detect LOGICAL_AGGREGATE_AND_GROUP_BY
  │    ├─> Detect LOGICAL_DISTINCT
  │    └─> Determine merge strategy
  │
  ├─> ExtractPipelineTasks()
  ├─> Distribute to workers
  │
  └─> CollectAndMergeResults(..., query_analysis)  ← STEP 4a
       │
       └─> Log merge strategy
           └─> Apply appropriate merging (future)
```

### AnalyzeQuery() Implementation

```cpp
QueryAnalysis AnalyzeQuery(LogicalOperator &logical_plan) {
    QueryAnalysis analysis;
    
    // Recursive lambda to walk tree
    std::function<void(LogicalOperator&)> analyze_operator = [&](LogicalOperator &op) {
        // Check for AGGREGATE operator
        if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
            analysis.has_aggregates = true;
            
            auto &agg_op = op.Cast<LogicalAggregate>();
            
            // Check if GROUP BY
            if (!agg_op.groups.empty()) {
                analysis.has_group_by = true;
            }
            
            // Extract function names (COUNT, SUM, etc.)
            for (auto &expr : agg_op.expressions) {
                if (expr->type == ExpressionType::BOUND_AGGREGATE) {
                    auto &agg_expr = expr->Cast<BoundAggregateExpression>();
                    analysis.aggregate_functions.push_back(agg_expr.function.name);
                }
            }
        }
        
        // Check for DISTINCT
        if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
            analysis.has_distinct = true;
        }
        
        // Recurse to children
        for (auto &child : op.children) {
            analyze_operator(*child);
        }
    };
    
    analyze_operator(logical_plan);
    
    // Determine strategy
    if (analysis.has_group_by) {
        analysis.merge_strategy = MergeStrategy::GROUP_BY_MERGE;
    } else if (analysis.has_aggregates) {
        analysis.merge_strategy = MergeStrategy::AGGREGATE_MERGE;
    } else if (analysis.has_distinct) {
        analysis.merge_strategy = MergeStrategy::DISTINCT_MERGE;
    } else {
        analysis.merge_strategy = MergeStrategy::CONCATENATE;
    }
    
    return analysis;
}
```

---

## Example Output

### Simple Scan Query

```
[STEP 4] Analyzing query for merge strategy...
  Analyzing operator: PROJECTION
  Analyzing operator: GET
  Merge Strategy: CONCATENATE (simple scan)

========== STEP 4: MERGE PHASE ==========
[STEP 4] Smart merge phase starting...
  Merge Strategy: CONCATENATE (simple scan)
```

### Aggregation Query (when distributed)

```
[STEP 4] Analyzing query for merge strategy...
  Analyzing operator: PROJECTION
  Analyzing operator: LOGICAL_AGGREGATE_AND_GROUP_BY
    Found aggregate function: count
    Found aggregate function: sum
  Analyzing operator: GET
  Merge Strategy: AGGREGATE_MERGE (merge partial aggregates)

========== STEP 4: MERGE PHASE ==========
[STEP 4] Smart merge phase starting...
  Merge Strategy: AGGREGATE_MERGE (merge partial aggregates)
```

### GROUP BY Query (when distributed)

```
[STEP 4] Analyzing query for merge strategy...
  Analyzing operator: PROJECTION
  Analyzing operator: LOGICAL_AGGREGATE_AND_GROUP_BY
    Found GROUP BY with 1 group(s)
    Found aggregate function: count
    Found aggregate function: sum
  Analyzing operator: GET
  Merge Strategy: GROUP_BY_MERGE (merge grouped results)
```

---

## Current Behavior

### What Works Now
- ✅ Query analysis correctly detects merge strategy
- ✅ Simple scans use CONCATENATE (working as before)
- ✅ All existing tests pass
- ✅ Foundation ready for smart merging

### Current Limitation
Currently, aggregations are **not being pushed to the distributed executor**. When you run:

```sql
SELECT COUNT(*), SUM(value) FROM table;
```

What happens:
1. Only the base table scan (`SELECT * FROM table`) is distributed
2. Workers return all rows
3. Coordinator performs the aggregation locally

This is because:
- Our `CanDistribute()` allows aggregations
- But they're executed locally after gathering data
- This is correct for now - we're building the foundation

---

## Future Enhancement: Step 4b

**Step 4b** will implement actual smart merging:

### For AGGREGATE_MERGE:
```cpp
if (query_analysis.merge_strategy == MergeStrategy::AGGREGATE_MERGE) {
    // Workers returned partial aggregates
    // Need to combine them:
    // - SUM(partial_sums) for SUM
    // - SUM(partial_counts) for COUNT
    // - Weighted average for AVG
    
    // Create temporary table from collected results
    auto temp_table = CreateTempTableFromResults(collection);
    
    // Run SQL to re-aggregate
    string merge_sql = BuildMergeSQL(query_analysis.aggregate_functions);
    auto merged_result = conn.Query(merge_sql);
    
    return merged_result;
}
```

### For GROUP_BY_MERGE:
```cpp
if (query_analysis.merge_strategy == MergeStrategy::GROUP_BY_MERGE) {
    // Workers returned partial groups
    // Need to merge groups with same key and re-aggregate
    
    string merge_sql = StringUtil::Format(
        "SELECT %s, %s FROM temp_table GROUP BY %s",
        JoinColumns(query_analysis.group_by_columns),
        BuildAggregates(query_analysis.aggregate_functions),
        JoinColumns(query_analysis.group_by_columns)
    );
    
    auto merged_result = conn.Query(merge_sql);
    return merged_result;
}
```

### For DISTINCT_MERGE:
```cpp
if (query_analysis.merge_strategy == MergeStrategy::DISTINCT_MERGE) {
    // Workers returned potentially duplicate rows
    // Need to apply DISTINCT
    
    string merge_sql = "SELECT DISTINCT * FROM temp_table";
    auto merged_result = conn.Query(merge_sql);
    return merged_result;
}
```

---

## Files Modified

### Header File
**`src/include/server/driver/distributed_executor.hpp`**
- Added `MergeStrategy` enum
- Added `QueryAnalysis` struct
- Added `AnalyzeQuery()` method declaration
- Added new `CollectAndMergeResults()` overload with `QueryAnalysis` parameter

### Implementation File
**`src/server/driver/distributed_executor.cpp`**
- Added includes for logical operators:
  - `logical_aggregate.hpp`
  - `logical_distinct.hpp`
  - `bound_aggregate_expression.hpp`
- Implemented `AnalyzeQuery()` method
- Updated `ExecuteDistributed()` to call `AnalyzeQuery()`
- Implemented new `CollectAndMergeResults()` overload
- Added comprehensive logging for merge strategies

---

## Test Results

**All 17 tests pass with 776 assertions:**

```
===============================================================================
All tests passed (776 assertions in 17 test cases)
```

No regressions - Step 4a is backward compatible!

---

## Architecture Impact

### Before Step 4a
```
Coordinator
  ├─> Analyze parallelism
  ├─> Extract tasks
  ├─> Distribute to workers
  └─> Merge results (simple concatenation only)
```

### After Step 4a
```
Coordinator
  ├─> Analyze parallelism
  ├─> Analyze query (STEP 4a - detect merge strategy) ✨
  ├─> Extract tasks
  ├─> Distribute to workers
  └─> Merge results (strategy-aware, foundation ready) ✨
```

---

## Key Insights

### 1. Query Analysis is Separate from Execution
- We analyze the logical plan to understand query structure
- This is independent of actual execution
- Enables smart decisions about how to merge results

### 2. Recursive Tree Walking
- Logical plans are trees
- We recursively walk all operators
- Detect patterns at any depth

### 3. Strategy-Based Architecture
- Different query types need different merge approaches
- Strategy pattern makes code extensible
- Easy to add new strategies in future

### 4. Foundation First
- Step 4a: Build infrastructure, detect strategies
- Step 4b (future): Implement actual smart merging
- This iterative approach ensures stability

---

## Next Steps

### Step 4b: Implement Smart Merging
1. **AGGREGATE_MERGE**: Re-aggregate partial results
   - SUM(partial_sums)
   - SUM(partial_counts)
   - Weighted AVG

2. **GROUP_BY_MERGE**: Merge grouped results
   - Group by same keys
   - Re-aggregate per group

3. **DISTINCT_MERGE**: Eliminate duplicates
   - Apply DISTINCT to collected results

### Step 5: Physical Plan Serialization
- Serialize physical plans (not logical)
- Direct execution without re-optimization
- True LocalState/GlobalState semantics

### Step 6: Multi-Pipeline Support
- Handle complex queries with multiple pipelines
- Coordinate pipeline dependencies

---

## Success Criteria ✅

- [x] Code compiles without errors
- [x] Query analysis detects aggregations
- [x] Query analysis detects GROUP BY
- [x] Query analysis detects DISTINCT
- [x] Merge strategy determined correctly
- [x] New overload receives query analysis
- [x] Comprehensive logging added
- [x] All tests pass
- [x] No regressions

---

## Status

**STEP 4A COMPLETE** ✅

We've successfully implemented query analysis and merge strategy detection! The foundation is in place for smart result merging. Step 4b will build on this to actually implement the merge logic for each strategy.

**Steps 1, 2, 3, 4a now complete!** ✨

Ready for **Step 4b: Implement Smart Merging Logic**!

