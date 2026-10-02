#pragma once

#include "distributed.pb.h"
#include "server/driver/client_registration.hpp"
#include "server/driver/distributed_flight_server_test_state.hpp"

#include <arrow/status.h>

namespace duckdb {

// Runs BEGIN, COMMIT, and ROLLBACK requests on a client's session, keeping retries of a lifecycle operation
// idempotent through the registration's finished transaction watermark.
class TransactionRequestHandler {
public:
	explicit TransactionRequestHandler(DistributedFlightServerTestState &test_state);

	arrow::Status Handle(const distributed::DistributedRequest &req, ClientRegistration &registration,
	                     distributed::DistributedResponse &resp) DUCKDB_REQUIRES(registration.connection_mutex);

private:
	void ExecuteAction(const distributed::DistributedRequest &req, ClientRegistration &registration,
	                   distributed::DistributedResponse &resp) DUCKDB_REQUIRES(registration.connection_mutex);

	DistributedFlightServerTestState &test_state;
};

} // namespace duckdb
