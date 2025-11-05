# Duckherder - DuckDB Remote Execution Extension

Duckherder is a DuckDB extension that enables remote query execution on server using Apache Arrow Flight for data transfer. It allows you to seamlessly work with remote tables as if they were local, while maintaining DuckDB's familiar SQL interface.

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
│              │                           │
│              │ Arrow Flight Protocol     │
│              │                           │
└──────────────┼───────────────────────────┘
               │
               ▼
┌──────────────┴──────────────────────────┐
│         Server DuckDB Instance          │
│  ┌───────────────────────────────────┐  │
│  │   Duckling Storage                │  │
│  │   - Actual table data             │  │
│  │   - Query execution               │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

## Installation

### Building from Source

#### Prerequisites

1. Install VCPKG for dependency management:
```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

2. Build the extension:
```bash
git clone --recurse-submodules https://github.com/dentiny/duckdb-distributed-execution.git
cd duckdb-distributed-execution
export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake && CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make
```

The build produces:
- `./build/release/duckdb` - DuckDB shell with extension pre-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/duckherder/duckherder.duckdb_extension` - Loadable extension

## Usage

### Basic Setup

#### 1. Server Management

```sql
-- Start a distributed server on port 8815
SELECT duckherder_start_local_server(8815);

-- Stop the local distributed server
SELECT duckherder_stop_local_server();
```

#### 2. Attach to the Server

```sql
-- Attach to the duckherder server as database 'dh'
-- TODO: currently only support database dh
ATTACH DATABASE 'dh' (TYPE duckherder, server_host 'localhost', server_port 8815);
```

#### 3. Register Remote Tables

```sql
-- Register a remote table mapping
-- Syntax: duckherder_register_remote_table(local_name, remote_table_name)
PRAGMA duckherder_register_remote_table('my_table', 'my_table');
```

### Working with Remote Tables

#### Create Tables

```sql
CREATE TABLE dh.users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);
```

#### Insert Data

```sql
INSERT INTO dh.users VALUES 
    (1, 'Alice', 'alice@example.com', '2024-01-15 10:30:00'),
    (2, 'Bob', 'bob@example.com', '2024-01-16 14:20:00'),
    (3, 'Charlie', 'charlie@example.com', '2024-01-17 09:15:00');
```

#### Query Data

```sql
-- Simple SELECT
SELECT * FROM dh.users WHERE id > 1;

-- Aggregations
SELECT COUNT(*) as user_count FROM dh.users;
```

#### Update and Delete

```sql
-- Delete specific rows
DELETE FROM dh.users WHERE id = 1;

-- Delete with conditions
DELETE FROM dh.users WHERE created_at < '2024-01-16';
```

#### Alter Tables

```sql
-- Add a column
ALTER TABLE dh.users ADD COLUMN age INTEGER;

-- Add a column with default value
ALTER TABLE dh.users ADD COLUMN status VARCHAR DEFAULT 'active';

-- Rename a column
ALTER TABLE dh.users RENAME COLUMN email TO email_address;

-- Drop a column
ALTER TABLE dh.users DROP COLUMN age;

-- Conditional operations
ALTER TABLE dh.users ADD COLUMN IF NOT EXISTS phone VARCHAR;
ALTER TABLE dh.users DROP COLUMN IF EXISTS temp_column;
```

#### Indexes

```sql
-- Create an index
CREATE INDEX idx_users_email ON dh.users(email);

-- Create multi-column index
CREATE INDEX idx_users_name_email ON dh.users(name, email);

-- Drop an index
DROP INDEX dh.idx_users_email;

-- Conditional drop
DROP INDEX IF EXISTS dh.idx_users_email;
```

#### Drop Tables

```sql
-- Drop a table
DROP TABLE dh.users;

-- Conditional drop
DROP TABLE IF EXISTS dh.users;
```

### Advanced Features

#### Load Extensions on Server

```sql
-- Load an extension on the remote server
SELECT duckherder_load_extension('parquet');
SELECT duckherder_load_extension('json');
```

#### Query Statistics

```sql
-- View query history and statistics
SELECT * FROM duckherder_get_query_history();

-- Clear query history
SELECT duckherder_clear_query_recorder_stats();
```

#### Unregister Tables

```sql
-- Unregister a remote table
PRAGMA duckherder_unregister_remote_table('my_table');
```

### Supported Data Types

Duckherder supports all DuckDB primitive data types including:

- **Numeric Types**: `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `HUGEINT`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `UHUGEINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **String Types**: `VARCHAR`, `TEXT`
- **Binary Types**: `BLOB`
- **Boolean**: `BOOLEAN`
- **Date/Time Types**: `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP_NS`, `INTERVAL`
- **Special Types**: `UUID`
- **NULL values**: Fully supported across all types

**TODO**: Add support for complex types (LIST, STRUCT, MAP, ARRAY, UNION).

## Roadmap

### ✅ Implemented Features

#### Core Functionality
- [x] **Arrow Flight Protocol Integration** - High-performance data transfer between client and server
- [x] **Custom Catalog System** - Client-side (Duckherder) and server-side (Duckling) catalogs
- [x] **Remote Table Registration** - Register and unregister remote tables with pragma functions
- [x] **Local Server Management** - Start and stop distributed servers programmatically

#### Table Operations
- [x] **CREATE TABLE** - Create tables on remote servers with full schema support
- [x] **DROP TABLE** - Drop remote tables with IF EXISTS support
- [x] **INSERT INTO** - Insert data into remote tables
- [x] **SELECT** - Query remote tables with full SQL support
- [x] **DELETE** - Delete rows from remote tables with WHERE conditions

#### Schema Modifications
- [x] **ALTER TABLE ADD COLUMN** - Add new columns with optional DEFAULT values
- [x] **ALTER TABLE DROP COLUMN** - Remove columns with IF EXISTS support
- [x] **ALTER TABLE RENAME COLUMN** - Rename existing columns
- [x] **ALTER TABLE Conditional Operations** - ADD IF NOT EXISTS, DROP IF EXISTS

#### Index Support
- [x] **CREATE INDEX** - Create indexes on remote tables
- [x] **DROP INDEX** - Drop indexes with IF EXISTS support
- [x] **Multi-column Indexes** - Support for composite indexes

**TODO**: Enhance index support with more advanced indexing features.

#### Data Types
- [x] **All Numeric Types** - Support for TINYINT through HUGEINT and all unsigned variants
- [x] **Floating Point Types** - FLOAT and DOUBLE with full precision
- [x] **String Types** - VARCHAR, TEXT
- [x] **Binary Types** - BLOB
- [x] **Boolean Type** - BOOLEAN
- [x] **Date/Time Types** - DATE, TIME, TIMESTAMP with all precision variants
- [x] **INTERVAL Type** - Time interval support
- [x] **UUID Type** - UUID support
- [x] **DECIMAL Type** - Arbitrary precision decimal numbers
- [x] **NULL Handling** - Full NULL support across all types

#### Additional Features
- [x] **Query History Tracking** - Record and retrieve query execution statistics
- [x] **Extension Loading** - Load DuckDB extensions on remote servers
- [x] **Transaction Support** - Basic transaction management
- [x] **Error Handling** - Comprehensive error reporting and propagation

### 🚧 Planned Features

#### Performance Optimizations
- [ ] **Query Result Caching** - Cache frequently accessed query results
- [ ] **Connection Pooling** - Reuse Flight connections for better performance
- [ ] **Parallel Query Execution** - Execute queries across multiple servers simultaneously
- [ ] **Predicate Pushdown Optimization** - Push filters to remote servers before data transfer
- [ ] **Batch Insert Optimization** - Optimize bulk insert operations

#### Advanced Query Features
- [ ] **JOIN Optimization** - Intelligent join execution across local and remote tables
- [ ] **Subquery Support** - Full support for subqueries involving remote tables
- [ ] **Common Table Expressions (CTEs)** - CTE support with remote tables
- [ ] **Window Functions** - Advanced window function support on remote data
- [ ] **Set Operations** - UNION, INTERSECT, EXCEPT across remote tables

#### Schema Management
- [ ] **ALTER TABLE Constraints** - Support for PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK constraints
- [ ] **CREATE/DROP SCHEMA** - Schema management on remote servers
- [ ] **CREATE/DROP VIEW** - View support for remote tables
- [ ] **Materialized Views** - Cached query results on remote servers

#### Security & Authentication
- [ ] **SSL/TLS Encryption** - Encrypted communication between client and server
- [ ] **User Authentication** - Username/password authentication
- [ ] **Token-based Authentication** - JWT or API token support
- [ ] **Role-based Access Control** - Fine-grained permissions for tables and operations
- [ ] **Audit Logging** - Comprehensive audit trail for all operations

#### Distributed Features
- [ ] **Multi-server Support** - Connect to multiple remote servers simultaneously
- [ ] **Data Partitioning** - Automatic data sharding across multiple servers
- [ ] **Load Balancing** - Distribute queries across multiple server instances
- [ ] **Replication Support** - Data replication for high availability
- [ ] **Automatic Failover** - Graceful handling of server failures

#### Transaction Management
- [ ] **Multi-statement Transactions** - BEGIN, COMMIT, ROLLBACK support
- [ ] **Distributed Transactions** - Two-phase commit for cross-server transactions
- [ ] **Savepoints** - Transaction savepoint support
- [ ] **Isolation Levels** - Configurable transaction isolation

#### Import/Export
- [ ] **COPY TO/FROM** - Efficient bulk import/export
- [ ] **Parquet Integration** - Direct Parquet file access on remote servers
- [ ] **CSV Import/Export** - Streamlined CSV operations
- [ ] **JSON Support** - JSON data import/export

#### Monitoring & Management
- [ ] **Server Health Monitoring** - Real-time server status and metrics
- [ ] **Query Performance Metrics** - Detailed query execution statistics
- [ ] **Resource Usage Tracking** - Monitor CPU, memory, network usage
- [ ] **Slow Query Log** - Identify and log slow-running queries
- [ ] **Configuration Management** - Remote server configuration via SQL

#### Client Libraries
- [ ] **Python Client** - Native Python API for Duckherder
- [ ] **REST API** - HTTP API for language-agnostic access
- [ ] **JDBC Driver** - Standard JDBC connectivity
- [ ] **ODBC Driver** - ODBC support for compatibility

#### Developer Features
- [ ] **Query Plan Visualization** - Visual representation of distributed query plans
- [ ] **Debug Mode** - Detailed logging and tracing for development
- [ ] **Mock Server** - Testing framework with mock remote servers
- [ ] **Benchmark Suite** - Performance benchmarking tools

#### Data Management
- [ ] **Table Statistics** - Collect and utilize table statistics for optimization
- [ ] **VACUUM/ANALYZE** - Remote table maintenance operations
- [ ] **Table Partitioning** - Partition tables across servers
- [ ] **Compression** - Configurable data compression for transfers

## Testing

Run the test suite:

```bash
make test
```

Run specific test files:

```bash
./build/release/test/unittest test/sql/registration.test
./build/release/test/unittest test/sql/remote_execution.test
./build/release/test/unittest test/sql/type_support.test
```

## Performance Considerations

1. **Network Latency**: Operations on remote tables incur network overhead. Use indexes and WHERE clauses to minimize data transfer.

2. **Data Transfer**: Arrow Flight provides efficient columnar data transfer, but large result sets will still require significant bandwidth.

3. **Query Planning**: Complex queries may benefit from splitting logic between local and remote operations.

4. **Batching**: Batch multiple INSERT operations when possible to reduce round trips.

## Troubleshooting

### Connection Issues

```sql
-- Verify server is running
SELECT duckherder_start_local_server(8815);

-- Check connection with ATTACH
ATTACH DATABASE 'dh' (TYPE duckherder, server_host 'localhost', server_port 8815);
```

### Table Registration Issues

```sql
-- Ensure table is registered before operations
PRAGMA duckherder_register_remote_table('my_table', 'my_table');

-- Verify registration by attempting to query
SELECT * FROM dh.my_table LIMIT 1;
```
