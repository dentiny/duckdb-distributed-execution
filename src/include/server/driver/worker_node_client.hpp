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
	explicit WorkerNodeClient(const string &location_p);

	// Connect to the worker.
	arrow::Status Connect();

	// Starts a partitioned query task on the worker and returns the stream of its result batches. Errors raised before
	// the worker sends its schema, e.g. for an invalid plan, are returned here; later ones by the stream.
	arrow::Result<std::unique_ptr<arrow::flight::FlightStreamReader>>
	ExecutePartition(const distributed::ExecutePartitionRequest &request);

private:
	string location;
	std::unique_ptr<arrow::flight::FlightClient> client;
};

} // namespace duckdb
