#include "server/driver/distributed_flight_server_test_state.hpp"

namespace duckdb {

void DistributedFlightServerTestState::SetClientLeaseTimeout(std::chrono::milliseconds timeout) {
	client_lease_timeout_ms = timeout.count();
}

std::chrono::milliseconds DistributedFlightServerTestState::GetClientLeaseTimeout() const {
	return std::chrono::milliseconds(client_lease_timeout_ms.load());
}

void DistributedFlightServerTestState::InjectFault(DistributedFlightServerTestFault fault, uint32_t count) {
	switch (fault) {
	case DistributedFlightServerTestFault::COMMIT_RESPONSE:
		fail_commit_responses = count;
		break;
	case DistributedFlightServerTestFault::EXECUTE_STATEMENT_RESPONSE:
		fail_execute_statement_responses = count;
		break;
	case DistributedFlightServerTestFault::SCAN_RESPONSE:
		fail_scan_responses = count;
		break;
	case DistributedFlightServerTestFault::UNKNOWN_TRANSACTION_RESPONSE:
		unknown_transaction_responses = count;
		break;
	}
}

bool DistributedFlightServerTestState::ConsumeFault(atomic<uint32_t> &remaining_faults) {
	auto remaining = remaining_faults.load();
	while (remaining > 0 && !remaining_faults.compare_exchange_weak(remaining, remaining - 1)) {
	}
	return remaining > 0;
}

bool DistributedFlightServerTestState::ShouldFailCommitResponse() {
	return ConsumeFault(fail_commit_responses);
}

bool DistributedFlightServerTestState::ShouldFailExecuteStatementResponse() {
	return ConsumeFault(fail_execute_statement_responses);
}

bool DistributedFlightServerTestState::ShouldFailScanResponse() {
	return ConsumeFault(fail_scan_responses);
}

bool DistributedFlightServerTestState::ShouldReturnUnknownTransactionResponse() {
	return ConsumeFault(unknown_transaction_responses);
}

void DistributedFlightServerTestState::RecordTransactionRequest() {
	transaction_request_count++;
}

uint64_t DistributedFlightServerTestState::GetTransactionRequestCount() const {
	return transaction_request_count.load();
}

} // namespace duckdb
