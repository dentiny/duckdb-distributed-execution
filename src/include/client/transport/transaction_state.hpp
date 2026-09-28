#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "transaction.pb.h"

#include <cstdint>

namespace duckdb {

class ClientContext;

inline constexpr uint64_t INVALID_TRANSACTION_ID = 0;
inline constexpr uint64_t INITIAL_TRANSACTION_ID = 1;
inline constexpr uint64_t INITIAL_REQUEST_SEQUENCE = 1;

// Client-side state shared by explicit transactions and single-RPC autocommit operations.
struct DistributedTransactionState {
	optional_ptr<ClientContext> context;
	uint64_t transaction_id = INVALID_TRANSACTION_ID;
	uint64_t next_transaction_id = INITIAL_TRANSACTION_ID;
	uint64_t next_request_sequence = INITIAL_REQUEST_SEQUENCE;
	bool requires_rollback = false;
	bool pending_autocommit_operation = false;
	distributed::TransactionAction pending_action = distributed::TRANSACTION_ACTION_UNSPECIFIED;
};

} // namespace duckdb
