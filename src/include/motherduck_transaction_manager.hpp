#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class MotherduckTransactionManager : public TransactionManager {
public:
	MotherduckTransactionManager(AttachedDatabase &db, Catalog &catalog);

	~MotherduckTransactionManager();

	Transaction &StartTransaction(ClientContext &context) override;

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;

	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	Catalog &catalog;
	mutex lock;
	reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

} // namespace duckdb
