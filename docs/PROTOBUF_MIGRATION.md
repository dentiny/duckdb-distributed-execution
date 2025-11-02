# Migrating to Protobuf for Control Messages

## Why Protobuf?

Since we're already using Arrow Flight (which uses gRPC/protobuf), using protobuf for control messages gives us:

1. **No new dependencies** - already included via Arrow Flight
2. **Schema evolution** - add fields without breaking compatibility
3. **Type safety** - compiler catches errors
4. **Self-documenting** - .proto files are the API contract
5. **Cross-language** - easy to add Python/Java/Go clients later

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Client                             │
└─────────────────────────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
  Control Messages              Data Transfer
  (Protobuf/gRPC)              (Arrow Flight)
  - Table metadata             - RecordBatches
  - SQL commands               - Streaming data
  - Status/errors              - Zero-copy
        │                             │
        └──────────────┬──────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│              Distributed Server                         │
└─────────────────────────────────────────────────────────┘
```

## Integration Steps

### 1. Generate Protobuf Code

Add to `CMakeLists.txt`:

```cmake
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Generate protobuf sources
set(PROTO_FILES
    src/proto/distributed.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Add to extension sources
set(EXTENSION_SOURCES
  ${EXTENSION_SOURCES}
  ${PROTO_SRCS}
)

target_include_directories(${EXTENSION_NAME} PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}  # For generated headers
)

target_link_libraries(${EXTENSION_NAME}
  protobuf::libprotobuf
  gRPC::grpc++
)
```

### 2. Usage Example

**Before (custom serialization):**
```cpp
DistributedRequest req;
req.type = RequestType::SCAN_TABLE;
req.table_name = "my_table";
req.limit = 1000;
auto data = req.Serialize();  // Manual memcpy
```

**After (protobuf):**
```cpp
duckdb::distributed::DistributedRequest req;
req.set_type(duckdb::distributed::SCAN_TABLE);
req.set_table_name("my_table");
req.set_limit(1000);
std::string data = req.SerializeAsString();  // Type-safe
```

### 3. Versioning Example

When you need to add a new field:

```proto
message DistributedRequest {
  // ... existing fields ...
  
  // New field - old clients will ignore this
  string query_id = 11;  // Added in v2
  
  // Deprecated field - still works but marked
  string old_field = 12 [deprecated = true];
}
```

## Benefits for Your TODOs

Looking at your code comments:

```cpp
// TODO(hjiang): We should use arrow as communication protocol 
// instead of a plain sql statement.
```

With protobuf + Arrow Flight:
- **Control**: Use protobuf for metadata (what to insert, where)
- **Data**: Use Arrow RecordBatch for the actual rows
- **Result**: Best of both worlds - type-safe control, efficient data

## Migration Path

1. ✅ Define `.proto` schema (done - see `src/proto/distributed.proto`)
2. Add protobuf generation to CMake
3. Create wrapper functions to bridge old/new APIs
4. Gradually migrate each endpoint
5. Remove old custom serialization

## Alternative: Keep It Simple?

If you want to stay with custom serialization because:
- The protocol is very stable
- Only internal use (no external clients)
- You value simplicity

Then that's totally valid! But since you already have protobuf via Arrow Flight, the additional complexity is minimal and the benefits are significant.

