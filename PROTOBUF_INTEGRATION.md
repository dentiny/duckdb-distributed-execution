# How to Integrate Protobuf for Control Messages

## TL;DR - Recommendation: YES, use Protobuf

**Reason**: You already have protobuf installed (via Arrow Flight), so there's zero overhead and you get:
- Type safety
- Schema evolution
- Better debugging
- Self-documenting API

## Step-by-Step Integration

### Step 1: Add protobuf generation to CMakeLists.txt

Update `/home/vscode/duckdb-distributed-execution/CMakeLists.txt`:

```cmake
# After line 13 (after find_package(ArrowFlight...))
find_package(Protobuf REQUIRED)

# After line 19 (after include_directories)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/proto)  # For generated headers

# Before setting EXTENSION_SOURCES (around line 22)
# Generate protobuf code
set(PROTO_FILES src/proto/distributed.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

set(EXTENSION_SOURCES
  ${PROTO_SRCS}  # ADD THIS - generated protobuf sources
  src/base_query_recorder.cpp
  src/distributed_delete.cpp
  # ... rest of your sources ...
)
```

### Step 2: Update target_link_libraries (around line 50)

```cmake
target_link_libraries(${EXTENSION_NAME} 
  OpenSSL::SSL 
  OpenSSL::Crypto
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  protobuf::libprotobuf  # ADD THIS
)

target_link_libraries(${LOADABLE_EXTENSION_NAME} 
  OpenSSL::SSL 
  OpenSSL::Crypto
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  protobuf::libprotobuf  # ADD THIS
)
```

### Step 3: Update your code to use protobuf

**Replace `distributed_protocol.cpp`:**

```cpp
#include "distributed.pb.h"  // Generated from .proto

// Old way (manual serialization)
DistributedRequest req;
req.type = RequestType::SCAN_TABLE;
auto data = req.Serialize();

// New way (protobuf)
duckdb::distributed::DistributedRequest proto_req;
proto_req.set_type(duckdb::distributed::SCAN_TABLE);
proto_req.set_table_name("my_table");
proto_req.set_limit(1000);

std::string serialized = proto_req.SerializeAsString();

// To deserialize
duckdb::distributed::DistributedRequest received;
received.ParseFromString(serialized);
```

### Step 4: Update Flight server integration

In `distributed_flight_server.cpp`:

```cpp
arrow::Status DistributedFlightServer::DoAction(
    const arrow::flight::ServerCallContext &context,
    const arrow::flight::Action &action,
    std::unique_ptr<arrow::flight::ResultStream> *result) {
    
    // Parse protobuf request
    duckdb::distributed::DistributedRequest req;
    if (!req.ParseFromArray(action.body->data(), action.body->size())) {
        return arrow::Status::Invalid("Failed to parse request");
    }
    
    // Handle request
    duckdb::distributed::DistributedResponse resp;
    resp.set_success(true);
    // ... handle request ...
    
    // Serialize protobuf response
    std::string resp_data = resp.SerializeAsString();
    auto buffer = arrow::Buffer::FromString(resp_data);
    // ... return response ...
}
```

## Build and Test

```bash
cd /home/vscode/duckdb-distributed-execution
rm -rf build/reldebug  # Clean build
export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake
OVERRIDE_GIT_DESCRIBE=v1.4.1 CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make reldebug
```

## Benefits You'll Get

1. **Type Safety**: Compiler errors instead of runtime bugs
2. **Versioning**: Add `query_id`, `timeout`, etc. later without breaking old clients
3. **Validation**: Protobuf validates required fields automatically
4. **Documentation**: `.proto` file is the source of truth
5. **Debugging**: Protobuf messages are easier to inspect than raw bytes
6. **Future-proof**: Easy to add Python/Go/Java clients later

## When NOT to use Protobuf

Keep custom serialization if:
- ❌ You need absolute minimal latency (nanoseconds matter)
- ❌ Protocol will NEVER change
- ❌ You love writing manual memcpy code 😄

Otherwise: **Use Protobuf** ✅

## Final Architecture

```
┌────────────────────────────────────────────┐
│         DuckDB Client                       │
│  ┌──────────────┐  ┌──────────────┐        │
│  │ Control Msgs │  │  Data Query  │        │
│  │  (Protobuf)  │  │   (SQL)      │        │
│  └──────┬───────┘  └───────┬──────┘        │
└─────────┼──────────────────┼───────────────┘
          │                  │
          ▼                  ▼
    ┌─────────────────────────────────┐
    │    Arrow Flight Server           │
    │  ┌──────────┐  ┌──────────────┐ │
    │  │ DoAction │  │    DoGet     │ │
    │  │(Protobuf)│  │(RecordBatch) │ │
    │  └─────┬────┘  └───────┬──────┘ │
    │        │               │         │
    │        ▼               ▼         │
    │  ┌──────────────────────────┐   │
    │  │    DuckDB Instance       │   │
    │  └──────────────────────────┘   │
    └─────────────────────────────────┘
```

**Control Messages** (metadata, commands) → Protobuf
**Data Transfer** (actual rows) → Arrow RecordBatch

This is the industry-standard approach! ✨

