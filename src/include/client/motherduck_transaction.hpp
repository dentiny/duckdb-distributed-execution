#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

// Forward declaration.
class Transaction;
class DuckTransactionManager;

class MotherduckTransaction : public Transaction {
public:
	MotherduckTransaction(DuckTransactionManager &manager, ClientContext &context, transaction_t start_time,
	                      transaction_t transaction_id, idx_t catalog_version);

	~MotherduckTransaction() override;

private:
	unique_ptr<DuckTransaction> duckdb_transaction;
};

} // namespace duckdb
