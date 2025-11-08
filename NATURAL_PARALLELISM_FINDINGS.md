# Natural Parallelism Investigation - Key Findings

## Summary
After extensive investigation, we've identified fundamental architectural limitations that prevent us from leveraging DuckDB's natural parallelism hints for distributed queries in the current architecture.

## The Goal
We wanted to query DuckDB's physical plan to understand:
- How many parallel tasks DuckDB would naturally create (`EstimatedThreadCount()`)
- Table statistics (cardinality, row count)
- Use this information to create intelligent partitioning strategies

## What We Discovered

### Issue #1: API Misuse (FIXED)
**Problem**: We were calling `PhysicalPlanGenerator::CreatePlan()` directly without initializing the internal `physical_plan` member variable.

**Error**: `"Attempted to dereference unique_ptr that is NULL!"`

**Fix**: Use `PhysicalPlanGenerator::Plan()` instead, which properly initializes internal state. We clone the logical plan using `logical_plan.Copy(context)` since `Plan()` takes ownership.

### Issue #2: Transaction Context (CURRENT BLOCKER)
**Problem**: Physical plan generation requires an active transaction context.

**Error**: `"TransactionContext::ActiveTransaction called without active transaction"`

**Root Cause**: 
- `conn.ExtractPlan(sql)` creates a logical plan, possibly within its own transaction
- When we later try to generate a physical plan for analysis, we're outside that transaction
- Physical plan generation needs transaction context to access table statistics and metadata

## Why This Matters for Distributed Execution

In DuckDB's single-process execution:
1. Query is parsed → Logical plan
2. Optimizer runs within a transaction
3. Physical plan is generated (can access table statistics)
4. Execution happens within the same transaction

In our distributed architecture:
1. **Coordinator** extracts logical plan (possibly ending a transaction)
2. **Coordinator** tries to analyze physical plan (NO TRANSACTION - fails!)
3. **Workers** execute with their own transactions

## Current Status

**The tests are passing!** Distributed execution works correctly:
- ✅ 10,000 rows inserted
- ✅ Queries distributed across 4 workers
- ✅ Results correctly merged
- ✅ All data accounts for

The limitation is only in **optimization/analysis**:
- ❌ Cannot query DuckDB's natural parallelism hints
- ❌ Cannot use table statistics for intelligent range-based partitioning
- ✅ Manual mod-based partitioning `(rowid % N)` works reliably

## Possible Solutions (Future Work)

### Option 1: Query Workers for Statistics
Instead of trying to analyze on the coordinator, ask workers for their table statistics:
```
Coordinator: "What's the row count for table X?"
Worker: "10,000 rows"
Coordinator: Uses this to decide: Range partitioning [0-2500], [2501-5000], etc.
```

### Option 2: Metadata Cache
Maintain a metadata cache on the coordinator with table statistics from workers.

### Option 3: Accept Current Limitation
The current `(rowid % N)` partitioning is:
- Simple and reliable
- Works for any table
- Distributes load evenly
- Requires no statistics

For many workloads, this is sufficient!

### Option 4: Transaction Management
Wrap the entire distributed execution flow in a proper transaction context. This is complex and would require significant architectural changes.

## Recommendation

**For now, accept the limitation and focus on making the current system robust.**

The goal of "leveraging DuckDB's natural parallelism" is admirable, but it's blocked by fundamental transaction/architecture constraints. The current manual partitioning strategy:

1. ✅ Works correctly
2. ✅ Distributes load evenly  
3. ✅ Simple to understand and debug
4. ✅ No dependencies on statistics or transactions

Future enhancement: Add a statistics query API where coordinator can ask workers for table metadata, then use that for intelligent partitioning decisions.

## Test Evidence

From `large_table_partitioning.test`:
```
Query: SELECT * FROM large_table
Workers: 4
Natural Parallelism: 0  ← Cannot query (transaction limitation)
Intelligent Partitioning: NO  ← Falls back to modulo
Estimated Cardinality: 0  ← Cannot query (transaction limitation)

Worker 0: 2500 rows  ← Evenly distributed!
Worker 1: 2500 rows
Worker 2: 2500 rows
Worker 3: 2500 rows
FINAL RESULT: 10000 total rows  ← Correct!
```

The system works; we just can't optimize partitioning using DuckDB's internal hints.

