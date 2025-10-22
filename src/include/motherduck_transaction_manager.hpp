#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class MotherduckTransactionManager : public DuckTransactionManager {
public:
	MotherduckTransactionManager(AttachedDatabase &db);

	~MotherduckTransactionManager();

	Transaction &StartTransaction(ClientContext &context) override;

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;

	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	bool IsDuckTransactionManager() override {
		return true;
	}

private:
	unique_ptr<DuckTransactionManager> duckdb_transaction_manager;
};

} // namespace duckdb
