#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "transaction.pb.h"
#include "transaction_constants.hpp"

namespace duckdb {

class ClientContext;

// Client-side state shared by explicit transactions and single-RPC autocommit operations.
struct DistributedTransactionState {
	optional_ptr<ClientContext> context;
	uint64_t transaction_id = INVALID_TRANSACTION_ID;
	uint64_t next_transaction_id = INITIAL_TRANSACTION_ID;
	uint64_t next_request_sequence = INITIAL_REQUEST_SEQUENCE;
	bool requires_rollback = false;
	bool pending_autocommit_operation = false;
	distributed::TransactionAction pending_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;

	void ResetExplicitTransaction();
};

} // namespace duckdb
