# Duckherder - DuckDB Remote Execution Extension

Duckherder is a DuckDB extension build upon [storage extension](https://github.com/duckdb/duckdb/pull/6066), which enables remote query execution on server using [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) for data transfer. It allows you to seamlessly work with remote tables as if they were local, while maintaining DuckDB's familiar SQL interface.

## Overview

Duckherder implements a client-server architecture where:
- **Client (Duckherder)**: Coordinates queries and manages remote table references
- **Server (Duckling)**: Executes queries on local data and returns results via Arrow Flight

The extension transparently handles query routing, allowing you to run CREATE, SELECT, INSERT, DELETE, and ALTER operations on remote tables through DuckDB storage extension.

## Architecture

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
│  │   Duckling Storage                │  │
│  │   - Actual table data             │  │
│  │   - Query execution               │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

## Node-Based Distributed Execution Implementation Summary

### DuckDB Parallel Execution → Distributed Execution

| DuckDB Thread Model | Distributed Model | Implementation |
|---------------------|-------------------|----------------|
| Thread | Worker Node | Physical machine/process |
| LocalSinkState | Worker Result | QueryResult → Arrow batches |
| Sink() | Worker Execute | `HandleExecutePartition()` |
| GlobalSinkState | ColumnDataCollection | Coordinator's result collection |
| Combine() | CollectAndMergeResults() | Merging worker outputs |
| Finalize() | Return MaterializedQueryResult | Final result to client |

## 📊 Execution Flow

```
┌─────────────┐
│ COORDINATOR │ Extract query plan
└──────┬──────┘
       │
       ├─ Phase 1: Extract & validate logical plan
       ├─ Phase 2: Create partition plans (one per worker)
       ├─ Phase 3: Prepare result schema
       │
       ├──────────┬──────────┐
       ▼          ▼          ▼
   ┌────────┐ ┌────────┐ ┌────────┐
   │WORKER 0│ │WORKER 1│ │WORKER N│  LocalState execution
   └───┬────┘ └───┬────┘ └───┬────┘
       │          │          │
       │ Execute  │ Execute  │ Execute
       │ partition│ partition│ partition
       │ (Local)  │ (Local)  │ (Local)
       │          │          │
       └──────────┴──────────┘
              │
              ▼
       ┌─────────────┐
       │ COORDINATOR │  GlobalState aggregation
       │   Combine   │  Merge LocalState outputs
       │  Finalize   │  Return final result
       └─────────────┘
```


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
export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake && CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make
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
-- Register a remote table mapping
-- Syntax: duckherder_register_remote_table(local_table_name, remote_table_name)
PRAGMA duckherder_register_remote_table('my_table', 'my_table');

-- Unregister a remote table mapping.
-- Syntax: duckherder_unregister_remote_table(local_table_name)
PRGAMA duckherder_unregister_remote_table('my_table');
```

### Load Extensions on Server

```sql
-- Load an extension on the remote server
SELECT duckherder_load_extension('parquet');
SELECT duckherder_load_extension('json');
```

### Example on Working with Remote Tables

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

-- Create an index
CREATE INDEX idx_users_email ON dh.users(email);

-- Simple SELECT
SELECT * FROM dh.users WHERE id > 1;

-- Drop a table
DROP TABLE dh.users;
```

### Query Statistics

```sql
-- View query history and statistics.
SELECT * FROM duckherder_get_query_history();

-- Clear query history.
SELECT duckherder_clear_query_recorder_stats();
```

## Roadmap

### Table and index operations
- [x] Create/drop table
- [x] Create/drop index
- [x] Update table schema
- [ ] Update index

### Data type support
- [x] Primitive type support
- [ ] List type support
- [ ] Map type support
- [ ] Struct type support

### Distributed query support
- [ ] Distribute query on server side via `duckling` storage extension

### Multi-client support
- [ ] Query server authN and authZ
- [ ] Multiple duckdb instance support on servers

### Full write support
- [ ] Persist server-side database file
- [ ] Recover duckdb instance via database file

### Additional feature support
- [x] Query timing stats
- [ ] Query resource consumption
- [x] Support official extension install and load
- [ ] Support community extension install and load
