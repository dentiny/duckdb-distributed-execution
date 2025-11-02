# Protobuf with `oneof` - Clean Architecture

## Summary of Changes

✅ **Removed unnecessary wrapper layer** (`distributed_protocol.cpp` deleted)  
✅ **Using protobuf messages directly** - no conversion overhead  
✅ **Type-safe with `oneof`** - compiler enforces which fields are valid  

## Before (Wrapper Layer - REMOVED)

```cpp
// ❌ OLD: Unnecessary wrapper
Client → DistributedRequest struct → ToProto() → protobuf → serialize
Server → deserialize → protobuf → FromProto() → DistributedRequest struct
```

**Problems:**
- Extra conversion overhead
- Dual maintenance (struct + protobuf)
- Type safety lost in conversion layer

## After (Direct Protobuf - CURRENT)

```cpp
// ✅ NEW: Use protobuf directly
Client → protobuf message → serialize
Server → deserialize → protobuf message
```

## Protobuf Schema (with `oneof`)

```proto
// Client requests - type-safe with oneof
message DistributedRequest {
  oneof request {
    ExecuteSQLRequest execute_sql = 1;
    CreateTableRequest create_table = 2;
    DropTableRequest drop_table = 3;
    ScanTableRequest scan_table = 4;
    DeleteDataRequest delete_data = 5;
    TableExistsRequest table_exists = 6;
  }
  map<string, string> metadata = 10;
}

// Server responses - type-safe with oneof
message DistributedResponse {
  bool success = 1;
  string error_message = 2;
  
  oneof response {
    ExecuteSQLResponse execute_sql = 3;
    CreateTableResponse create_table = 4;
    DropTableResponse drop_table = 5;
    ScanTableResponse scan_table = 6;
    DeleteDataResponse delete_data = 7;
    TableExistsResponse table_exists = 8;
  }
  map<string, string> metadata = 10;
}
```

## Client-Side Usage

```cpp
// Create a scan request
distributed::DistributedRequest request;
auto* scan = request.mutable_scan_table();  // Type-safe!
scan->set_table_name("my_table");
scan->set_limit(1000);
scan->set_offset(0);

// Serialize and send
std::string data = request.SerializeAsString();
// Send via Arrow Flight DoGet...
```

## Server-Side Usage

```cpp
// Receive and deserialize
distributed::DistributedRequest request;
request.ParseFromArray(data, size);

// Type-safe dispatch using oneof
switch (request.request_case()) {
  case distributed::DistributedRequest::kScanTable:
    HandleScan(request.scan_table());  // Type-safe access!
    break;
  case distributed::DistributedRequest::kExecuteSql:
    HandleExecuteSQL(request.execute_sql());
    break;
  // ...
}
```

## Benefits of `oneof`

### 1. **Type Safety**
```cpp
// ✅ Compiler enforces only one request type
request.mutable_scan_table()->set_table_name("foo");
request.mutable_execute_sql()->set_sql("SELECT...");  
// ^ Only the last one is set - previous is cleared

// ✅ Type-safe checking
if (request.has_scan_table()) {
  auto table = request.scan_table().table_name();
}
```

### 2. **Clear Intent**
```proto
// OLD: Which fields are used together? 🤔
message Request {
  string sql = 1;           // For execute_sql? create_table?
  string table_name = 2;    // For scan? drop? delete?
  uint64 limit = 3;         // Only for scan?
}

// NEW: Crystal clear! ✨
message Request {
  oneof request {
    ScanTableRequest scan_table = 1;  // Has: table_name, limit, offset
    ExecuteSQLRequest execute_sql = 2; // Has: sql
  }
}
```

### 3. **Efficient Wire Format**
- Only one variant is serialized
- No wasted bytes for unused fields
- Protobuf handles serialization efficiently

### 4. **Easy Evolution**
```proto
// Add new request type - doesn't break old clients
message DistributedRequest {
  oneof request {
    ScanTableRequest scan_table = 1;
    // ... existing types ...
    NewFeatureRequest new_feature = 10;  // ✅ Add safely
  }
}
```

## Handler Signatures (Type-Safe)

```cpp
// OLD: Generic handler, unclear which fields are used
arrow::Status HandleScan(const DistributedRequest &req, DistributedResponse &resp);
// What fields in req are valid? 🤔

// NEW: Specific types, crystal clear!
arrow::Status HandleScan(
  const distributed::ScanTableRequest &req,      // ✅ Only scan fields
  distributed::DistributedResponse &resp
);
```

## Architecture Diagram

```
┌─────────────────────────────────────────────┐
│            Client Application                │
│  ┌───────────────────────────────────────┐  │
│  │  distributed::DistributedRequest      │  │
│  │  • mutable_scan_table()               │  │
│  │  • mutable_execute_sql()              │  │
│  │  • SerializeAsString()                │  │
│  └─────────────┬─────────────────────────┘  │
└────────────────┼────────────────────────────┘
                 │ Protobuf bytes
                 ▼
        ┌────────────────────┐
        │   Arrow Flight     │
        │   (gRPC + Arrow)   │
        └────────┬───────────┘
                 │ Protobuf bytes
                 ▼
┌────────────────┼────────────────────────────┐
│                ▼                             │
│  ┌───────────────────────────────────────┐  │
│  │  distributed::DistributedRequest      │  │
│  │  • ParseFromArray()                   │  │
│  │  • request_case()  ← oneof switch     │  │
│  │  • scan_table()    ← type-safe access │  │
│  └───────────────────────────────────────┘  │
│            Flight Server                     │
└──────────────────────────────────────────────┘
```

## Files Modified

### Deleted
- ❌ `src/distributed_protocol.cpp` - No longer needed!

### Updated
- ✅ `src/proto/distributed.proto` - Added `oneof` and specific request/response types
- ✅ `src/include/distributed_protocol.hpp` - Now just includes protobuf header
- ✅ `src/distributed_flight_server.cpp` - Uses protobuf directly
- ✅ `src/include/distributed_flight_server.hpp` - Type-safe handler signatures
- ✅ `CMakeLists.txt` - Removed distributed_protocol.cpp

## Migration Guide

### For New Code
```cpp
// ✅ DO: Use protobuf directly
distributed::DistributedRequest request;
auto* scan = request.mutable_scan_table();
scan->set_table_name("users");

// ❌ DON'T: Create wrapper structs
// DistributedRequest req;  // OLD - deleted!
// req.type = RequestType::SCAN_TABLE;
```

### Pattern Matching
```cpp
// Use switch on request_case() for dispatch
switch (request.request_case()) {
  case distributed::DistributedRequest::kScanTable:
    // request.scan_table() is valid here
    ProcessScan(request.scan_table());
    break;
  case distributed::DistributedRequest::REQUEST_NOT_SET:
    return Error("No request set");
}
```

## Conclusion

**Before:** ~200 lines of wrapper code  
**After:** 0 lines of wrapper code (deleted!)  

**Benefits:**
- ✅ Simpler codebase
- ✅ Better type safety
- ✅ No conversion overhead
- ✅ Industry-standard pattern
- ✅ Easier to maintain

This is the **correct way** to use protobuf! 🎯

