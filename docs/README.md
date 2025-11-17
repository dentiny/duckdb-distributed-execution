# Duckherder - DuckDB Remote and Distributed Execution Extension

Duckherder is a DuckDB extension built upon [storage extension](https://github.com/duckdb/duckdb/pull/6066) that enables (certain) distributed query execution across multiple worker nodes using [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) for efficient data transfer. It allows you to seamlessly work with remote tables and execute queries in parallel across distributed workers while maintaining DuckDB's familiar SQL interface.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Distributed Execution System](#distributed-execution-system)
- [Installation](#installation)
- [Usage](#usage)
- [Roadmap](#roadmap)

## Overview

Duckherder implements a client-server architecture with distributed query execution:
- **Client (Duckherder)**: Coordinates queries, manages remote table references, and initiates distributed execution
- **Server (Duckling)**: Consists of two components:
  - **Driver Node**: Analyzes queries, creates partition plans, and coordinates worker execution
  - **Worker Nodes**: Execute partitioned queries on local data and return results via Arrow Flight
- **Distributed Executor**: Runs on the driver node, partitions queries based on DuckDB's physical plan analysis, and distributes tasks to workers

The extension transparently handles query routing, allowing you to run CREATE, SELECT, INSERT, DELETE, and ALTER operations on remote tables through DuckDB's storage extension interface.

## Architecture

### High-Level System Architecture

```
┌─────────────────────────────────────────┐
│         Client DuckDB Instance          │
│  ┌───────────────────────────────────┐  │
│  │   Duckherder Catalog (dh)         │  │
│  │   - Remote table references       │  │
│  │   - Query routing logic           │  │
│  └───────────────────────────────────┘  │
│              │                          │
│              │ Arrow Flight Protocol    │
│              │                          │
└──────────────┼──────────────────────────┘
               │
┌──────────────┴──────────────────────────┐
│         Server DuckDB Instance(s)       │
│  ┌───────────────────────────────────┐  │
│  │   Driver Node                     │  │
│  │   - Distributed executor          │  │
│  │   - Query plan analysis           │  │
│  │   - Task coordination             │  │
│  └───────────┬───────────────────────┘  │
│              │                          │
│  ┌───────────┴───────────────────────┐  │
│  │   Worker Nodes                    │  │
│  │   - Actual table data             │  │
│  │   - Partitioned execution         │  │
│  │   - Result streaming              │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

### Distributed Execution Flow

```
┌─────────────┐
│ COORDINATOR │ Extract query plan
└──────┬──────┘
       │
       ├─ Phase 1: Extract & validate logical plan
       ├─ Phase 2: Analyze DuckDB's natural parallelism
       ├─ Phase 3: Create partition plans
       ├─ Phase 4: Prepare result schema
       │
       ├──────────┬──────────┬──────────┐
       ▼          ▼          ▼          ▼
   ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
   │WORKER 0│ │WORKER 1│ │WORKER 2│ │WORKER N│
   └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘
       │          │          │          │
       │ Execute  │ Execute  │ Execute  │ Execute
       │ partition│ partition│ partition│ partition
       │ (Local)  │ (Local)  │ (Local)  │ (Local)
       │          │          │          │
       └──────────┴──────────┴──────────┘
              │
              ▼
       ┌─────────────┐
       │ COORDINATOR │  Merge results
       │   Combine   │  Smart aggregation
       │  Finalize   │  Return final result
       └─────────────┘
```

## Distributed Execution System

### Partitioning Strategy

The distributed executor analyzes DuckDB's physical plan to create optimal partitions:

#### 1. **Natural Parallelism Analysis**
- Queries `EstimatedThreadCount()` from DuckDB's physical plan
- Understands how many parallel tasks DuckDB would naturally create
- Extracts cardinality and operator information

#### 2. **Partition Strategy Selection (Priority Order)**

The system uses a **three-tier hierarchy** to select the optimal partitioning strategy:

| Priority | Condition | Strategy | Example Predicate | Notes |
|----------|-----------|----------|-------------------|-------|
| **1st** | Row groups detected (cardinality known) | **ROW GROUP-BASED** | `WHERE rowid BETWEEN 0 AND 245759` | Assigns whole row groups to tasks |
| **2nd** | TABLE_SCAN + ≥100 rows/worker | **RANGE-BASED** | `WHERE rowid BETWEEN 0 AND 2499` | Contiguous ranges, not row-group-aligned |
| **3rd** | Small table / Non-TABLE_SCAN / Unknown cardinality | **MODULO** | `WHERE (rowid % node id) = 0` | Fallback for all other cases. TODO(hjiang): for small table no need to distribute, should directly execute on driver node. |

**Row group-based partitioning (preferred):**
- **Aligned with DuckDB's storage structure** (122,880 rows per group)
- Tasks process complete row groups (e.g., Task 0: groups [0,1], Task 1: groups [2,3])
- Optimal cache locality and I/O efficiency
- Respects DuckDB's natural parallelism boundaries

**Range-based partitioning (fallback):**
- Used when row groups can't be detected but cardinality is known
- Contiguous row access
- Not aligned with row group boundaries

**Modulo partitioning (final fallback):**
- Scattered row access (poor cache locality)
- Works for any table size and operator type
- Simple and always correct

#### 3. **How Row Group-Based Partitioning Works**

When row groups are detected (the common case), the system:
- Calculates total row groups: `cardinality / 122,880`
- Assigns whole row groups to tasks (e.g., Task 0 gets groups [0,1], Task 1 gets groups [2,3])
- Converts row group ranges to rowid ranges: `rowid BETWEEN (rg_start * 122880) AND (rg_end * 122880 - 1)`
- Workers scan complete row groups for optimal performance

### DuckDB Thread Model → Distributed Model Mapping

| DuckDB Thread Model | Distributed Model | Implementation |
|---------------------|-------------------|----------------|
| Thread | Worker Node | Physical machine/process |
| LocalSinkState | Worker Result | QueryResult → Arrow batches |
| Sink() | Worker Execute | `HandleExecutePartition()` |
| GlobalSinkState | ColumnDataCollection | Coordinator's result collection |
| Combine() | CollectAndMergeResults() | Merging worker outputs |
| Finalize() | Return MaterializedQueryResult | Final result to client |

## Installation

### Building from Source

#### Prerequisites

1. Install [VCPKG](https://duckdb.org/2024/03/22/dependency-management) for dependency management:
```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

2. Build the extension:
```bash
# Clone the repo.
git clone --recurse-submodules https://github.com/dentiny/duckdb-distributed-execution.git
cd duckdb-distributed-execution

# Build with release mode.
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make
```

The build produces:
- `./build/release/duckdb` - DuckDB shell with extension pre-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/duckherder/duckherder.duckdb_extension` - Loadable extension

## Usage

### Local Server Management

```sql
-- Start a distributed server on port 8815.
SELECT duckherder_start_local_server(8815);

-- Stop the local distributed server.
SELECT duckherder_stop_local_server();
```

### Attach to the Server

```sql
-- Attach to the duckherder server as database 'dh'
-- TODO(hjiang): currently only support database 'dh'
ATTACH DATABASE 'dh' (TYPE duckherder, server_host 'localhost', server_port 8815);
```

### Register and Unregister Remote Tables

```sql
-- Register a remote table mapping.
-- Syntax: duckherder_register_remote_table(local_table_name, remote_table_name)
PRAGMA duckherder_register_remote_table('my_table', 'my_table');

-- Unregister a remote table mapping.
-- Syntax: duckherder_unregister_remote_table(local_table_name)
PRAGMA duckherder_unregister_remote_table('my_table');
```

### Load Extensions on Server

```sql
-- Load an extension on the remote server
SELECT duckherder_load_extension('parquet');
SELECT duckherder_load_extension('json');
```

### Working with Remote Tables

```sql
-- Create a table.
CREATE TABLE dh.users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);

-- Insert data.
INSERT INTO dh.users VALUES 
    (1, 'Alice', 'alice@example.com', '2024-01-15 10:30:00'),
    (2, 'Bob', 'bob@example.com', '2024-01-16 14:20:00'),
    (3, 'Charlie', 'charlie@example.com', '2024-01-17 09:15:00');

-- Create an index.
CREATE INDEX idx_users_email ON dh.users(email);

-- Simple SELECT.
SELECT * FROM dh.users WHERE id > 1;

-- Distributed query, which automatically gets parallelized.
SELECT * FROM dh.large_table WHERE value > 1000;

-- Drop a table.
DROP TABLE dh.users;
```

### Query Execution Monitoring and Statistics

duckherder provides built-in stats (current not persisted anywhere) for executed queries, including start timestamp, executime duration and disribution mode.
```sql
D SELECT * FROM duckherder_get_query_execution_stats();
┌───────────────────────────────────────┬────────────────┬────────────────┬───────────────────┬──────────────────┬─────────────────────┬────────────────────────────┐
│                  sql                  │ execution_mode │ merge_strategy │ query_duration_ms │ num_workers_used │ num_tasks_generated │    execution_start_time    │
│                varchar                │    varchar     │    varchar     │       int64       │      int64       │        int64        │         timestamp          │
├───────────────────────────────────────┼────────────────┼────────────────┼───────────────────┼──────────────────┼─────────────────────┼────────────────────────────┤
│ SELECT * FROM distributed_basic_table │ DELEGATED      │ CONCATENATE    │                17 │                4 │                   1 │ 2025-11-17 02:10:27.482089 │
│ SELECT * FROM distributed_basic_table │ DELEGATED      │ CONCATENATE    │                 1 │                4 │                   1 │ 2025-11-17 02:10:30.436037 │
│ SELECT * FROM distributed_basic_table │ DELEGATED      │ CONCATENATE    │                 2 │                4 │                   1 │ 2025-11-17 02:10:33.675752 │
│ SELECT * FROM distributed_basic_table │ DELEGATED      │ CONCATENATE    │                 1 │                4 │                   1 │ 2025-11-17 02:10:36.992988 │
└───────────────────────────────────────┴────────────────┴────────────────┴───────────────────┴──────────────────┴─────────────────────┴────────────────────────────┘
```

#### Clear Query Statistics

```sql
-- Clear all recorded query history and statistics
SELECT duckherder_clear_query_recorder_stats();

-- Verify the history is cleared
SELECT COUNT(*) FROM duckherder_get_query_history();  -- Returns: 0
```

## Roadmap

### Table and Index Operations
- [x] Create/drop table
- [x] Create/drop index
- [x] Update table schema
- [ ] Update index

### Data Type Support
- [x] Primitive type support
- [ ] List type support
- [ ] Map type support
- [ ] Struct type support

### Distributed Query Support
- [x] Intelligent query partitioning
- [x] Natural parallelism analysis
- [x] Row group-aligned execution
- [x] Range-based partitioning
- [ ] Aggregation pushdown (infrastructure ready)
- [ ] GROUP BY distributed execution
- [ ] JOIN optimization (broadcast/co-partition)
- [ ] ORDER BY support (distributed sort)
- [x] Driver collect partition and execution stats

### Multi-Client Support
- [ ] Query server authentication and authorization
- [ ] Multiple DuckDB instance support on servers
- [ ] Connection pooling

### Full Write Support
- [ ] Persist server-side database file
- [ ] Recover DuckDB instance via database file
- [ ] Transaction support

### Additional Features
- [x] Query timing statistics
- [x] Comprehensive execution logging
- [x] Support official extension install and load
- [ ] Query resource consumption tracking
- [x] Support community extension install and load
- [ ] Dynamic worker scaling
- [ ] Query result caching
- [x] Util function to register driver node and worker nodes

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for development guidelines.

## License

See [LICENSE](../LICENSE) for license information.
