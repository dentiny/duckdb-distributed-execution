#pragma once

#include "distributed.pb.h"  // Generated protobuf header

namespace duckdb {

// Protocol definition for distributed execution
// Uses Arrow Flight for efficient data transfer
// Control messages use Protobuf for type safety and versioning

// Use protobuf messages directly - no wrappers needed!
// Client-side usage example:
//   distributed::DistributedRequest request;
//   auto* scan = request.mutable_scan_table();
//   scan->set_table_name("my_table");
//   scan->set_limit(100);
//   std::string serialized = request.SerializeAsString();

// Server-side usage example:
//   distributed::DistributedRequest request;
//   request.ParseFromString(data);
//   switch (request.request_case()) {
//     case distributed::DistributedRequest::kScanTable:
//       HandleScan(request.scan_table());
//       break;
//   }

} // namespace duckdb
