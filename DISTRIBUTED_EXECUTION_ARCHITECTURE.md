# Distributed Execution Architecture

This document describes the client-server architecture for distributed query execution using Apache Arrow Flight.

## Overview

The system uses **Apache Arrow Flight** for efficient RPC communication between the DuckDB client (extension) and the distributed execution server. Arrow Flight provides:

- **High-performance data transfer** using Arrow's columnar format
- **gRPC-based RPC** for reliable communication
- **Streaming support** for large result sets
- **Zero-copy transfers** where possible

## Architecture

```
┌─────────────────────┐                    ┌─────────────────────┐
│   DuckDB Client     │                    │  Flight Server      │
│   (Extension)       │                    │  (Remote)           │
│                     │                    │                     │
│  ┌──────────────┐   │                    │  ┌──────────────┐   │
│  │ Flight Client│◄──┼────── gRPC ───────┼─►│ Flight Server│   │
│  └──────────────┘   │                    │  └──────────────┘   │
│         │           │                    │         │           │
│         ▼           │                    │         ▼           │
│  ┌──────────────┐   │   Arrow RecordBatch│  ┌──────────────┐   │
│  │  Insert/Scan │   │◄───────────────────┼──│   DuckDB     │   │
│  │  Operations  │   │                    │  │   Instance   │   │
│  └──────────────┘   │                    │  └──────────────┘   │
└─────────────────────┘                    └─────────────────────┘
```

## Protocol

### Request Types

The system supports the following operations:

1. **EXECUTE_SQL** - Execute arbitrary SQL
2. **CREATE_TABLE** - Create a table on the server
3. **DROP_TABLE** - Drop a table from the server
4. **INSERT_DATA** - Insert data using Arrow RecordBatch
5. **SCAN_TABLE** - Scan table data and return Arrow RecordBatch
6. **DELETE_DATA** - Delete data from table
7. **TABLE_EXISTS** - Check if table exists

### Communication Methods

#### DoAction (Metadata Operations)
Used for operations that don't transfer bulk data:
- CREATE_TABLE
- DROP_TABLE
- TABLE_EXISTS
- EXECUTE_SQL (for DDL)

**Flow:**
1. Client serializes `DistributedRequest` to binary
2. Client calls `DoAction()` with serialized request
3. Server processes request and returns `DistributedResponse`

#### DoGet (Scan Operations)
Used for retrieving data:
- SCAN_TABLE

**Flow:**
1. Client serializes `DistributedRequest` with scan parameters
2. Client calls `DoGet()` with request as ticket
3. Server executes query and streams Arrow RecordBatches back
4. Client receives RecordBatchReader

#### DoPut (Insert Operations)
Used for bulk data insertion:
- INSERT_DATA

**Flow:**
1. Client prepares Arrow RecordBatch from DuckDB data
2. Client calls `DoPut()` with table schema
3. Client streams RecordBatch(es) to server
4. Server inserts data into DuckDB table
5. Server returns response via metadata

## Data Format

All data is transferred using **Apache Arrow's columnar format**:
- **RecordBatch**: A collection of columns with the same length
- **Schema**: Column names and types
- **Zero-copy**: Data can be passed without serialization where supported

### DuckDB ↔ Arrow Conversion

DuckDB has built-in Arrow integration:
- `QueryResult::ToArrowTable()` - Convert query results to Arrow
- `ArrowAppender` - Append Arrow data to DuckDB tables

## Components

### 1. Protocol Definition (`distributed_protocol.hpp`)

Defines request/response structures:
```cpp
struct DistributedRequest {
    RequestType type;
    string sql;
    string table_name;
    uint64_t limit;
    uint64_t offset;
};

struct DistributedResponse {
    bool success;
    string error_message;
    uint64_t rows_affected;
    bool exists;
};
```

### 2. Flight Server (`distributed_flight_server.hpp/cpp`)

Arrow Flight server that:
- Accepts RPC requests from clients
- Executes operations on local DuckDB instance
- Returns results in Arrow format
- Handles streaming for large datasets

**Key Methods:**
- `DoAction()` - Handle metadata operations
- `DoGet()` - Stream query results to client
- `DoPut()` - Receive data from client

### 3. Flight Client (`distributed_flight_client.hpp/cpp`)

Arrow Flight client that:
- Connects to remote server
- Sends requests to server
- Receives Arrow RecordBatches
- Integrates with DuckDB physical operators

**Key Methods:**
- `Connect()` - Establish connection to server
- `ExecuteSQL()` - Execute SQL on server
- `InsertData()` - Insert Arrow RecordBatch
- `ScanTable()` - Retrieve data as RecordBatch

### 4. Server Main (`distributed_server_main.cpp`)

Standalone executable that:
- Starts the Flight server
- Listens for connections
- Handles graceful shutdown

## Usage

### Starting the Server

```bash
# Default: 0.0.0.0:8815
./distributed_server

# Custom host and port
./distributed_server 127.0.0.1 9000
```

### Client Integration

The client is integrated into DuckDB physical operators:

#### Scan Operation
```cpp
auto client = make_uniq<DistributedFlightClient>("grpc://localhost:8815");
client->Connect();

std::shared_ptr<arrow::RecordBatchReader> reader;
client->ScanTable("my_table", 1000, 0, reader);

// Convert Arrow to DuckDB DataChunk
while (auto batch = reader->Next()) {
    // Process batch
}
```

#### Insert Operation
```cpp
auto client = make_uniq<DistributedFlightClient>("grpc://localhost:8815");
client->Connect();

// Convert DuckDB DataChunk to Arrow RecordBatch
std::shared_ptr<arrow::RecordBatch> batch = ConvertToArrow(chunk);

DistributedResponse response;
client->InsertData("my_table", batch, response);
```

## Dependencies

Updated `vcpkg.json`:
```json
{
  "dependencies": [
    { "name": "openssl" },
    {
      "name": "arrow",
      "features": ["filesystem", "flight", "parquet"]
    },
    { "name": "grpc" },
    { "name": "protobuf" }
  ]
}
```

## Next Steps

### TODO: Update Physical Operators

1. **PhysicalDistributedInsert**
   - Replace SQL string building with Arrow RecordBatch conversion
   - Use `DistributedFlightClient::InsertData()`

2. **DistributedTableScanFunction**
   - Use `DistributedFlightClient::ScanTable()` 
   - Convert Arrow RecordBatch back to DuckDB DataChunk

3. **PhysicalDistributedDelete**
   - Implement using DoAction or custom Flight method

### TODO: Optimizations

1. **Batching**: Send multiple RecordBatches for large inserts
2. **Compression**: Enable Arrow compression for network transfer
3. **Connection Pooling**: Reuse Flight connections
4. **Async Operations**: Use Arrow Flight's async API
5. **Predicate Pushdown**: Send filter predicates to server

### TODO: Error Handling

1. Network failures and retries
2. Server unavailability
3. Schema mismatches
4. Timeout handling

## Performance Considerations

**Advantages of Arrow Flight:**
- ~100x faster than JSON for large datasets
- Zero-copy where possible
- Streaming reduces memory usage
- Built-in compression support
- gRPC connection multiplexing

**Best Practices:**
- Use batching for bulk operations
- Enable compression for WAN scenarios
- Implement connection pooling
- Use async APIs for concurrent operations
- Monitor network bandwidth usage

## Testing

To test the implementation:

1. Start the server:
```bash
./distributed_server
```

2. Connect from DuckDB client:
```sql
-- Load the extension
LOAD 'motherduck';

-- Register remote table
CALL register_remote_table('my_table', 'grpc://localhost:8815', 'remote_table');

-- Scan will use Flight client
SELECT * FROM my_table;
```

## Security

**TODO**: Add authentication and encryption:
- TLS/SSL for gRPC connections
- Authentication tokens
- Authorization checks
- Network encryption

