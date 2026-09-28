#pragma once

#include "duckdb/common/atomic.hpp"

#include <chrono>
#include <cstdint>

namespace duckdb {

enum class DistributedFlightServerTestFault : uint8_t {
	COMMIT_RESPONSE,
	EXECUTE_STATEMENT_RESPONSE,
	SCAN_RESPONSE,
	UNKNOWN_TRANSACTION_RESPONSE
};

// Fault-injection and instrumentation state used by DistributedFlightServer tests.
struct DistributedFlightServerTestState {
	void SetClientLeaseTimeout(std::chrono::milliseconds timeout);
	std::chrono::milliseconds GetClientLeaseTimeout() const;

	void InjectFault(DistributedFlightServerTestFault fault, uint32_t count = 1);
	bool ShouldFailCommitResponse();
	bool ShouldFailExecuteStatementResponse();
	bool ShouldFailScanResponse();
	bool ShouldReturnUnknownTransactionResponse();

	void RecordTransactionRequest();
	uint64_t GetTransactionRequestCount() const;

private:
	bool ConsumeFault(atomic<uint32_t> &remaining);

	atomic<int64_t> client_lease_timeout_ms {30000};
	atomic<uint32_t> fail_commit_responses {0};
	atomic<uint32_t> fail_execute_statement_responses {0};
	atomic<uint32_t> fail_scan_responses {0};
	atomic<uint32_t> unknown_transaction_responses {0};
	atomic<uint64_t> transaction_request_count {0};
};

} // namespace duckdb
