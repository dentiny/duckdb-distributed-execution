#include "client/transport/transaction_state.hpp"

namespace duckdb {

void DistributedTransactionState::ResetExplicitTransaction() {
	transaction_id = INVALID_TRANSACTION_ID;
	next_request_sequence = INITIAL_REQUEST_SEQUENCE;
	requires_rollback = false;
	pending_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
}

} // namespace duckdb
