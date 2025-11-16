#pragma once

#include "distributed.pb.h"
#include "duckdb/common/string.hpp"

#include <arrow/flight/api.h>
#include <memory>

namespace duckdb {

// Client for communicating with worker nodes.
// Used by the driver to send tasks to workers and receive results.
class WorkerNodeClient {
public:
	explicit WorkerNodeClient(const string &location);

	// Connect to the worker.
	arrow::Status Connect();

	// Execute a partitioned query task on the worker.
	arrow::Status ExecutePartition(const distributed::ExecutePartitionRequest &request,
	                               std::unique_ptr<arrow::flight::FlightStreamReader> &stream);

private:
	string location;
	std::unique_ptr<arrow::flight::FlightClient> client;
};

} // namespace duckdb
