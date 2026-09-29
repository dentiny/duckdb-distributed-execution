#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

// Keep all transaction state in the manager registered with the attached database.
// DuckDB storage scans read its visibility timestamps directly.
class DucklingTransactionManager : public DuckTransactionManager {
public:
	explicit DucklingTransactionManager(AttachedDatabase &db);

	~DucklingTransactionManager();

	Transaction &StartTransaction(ClientContext &context) override;

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;

	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	bool IsDuckTransactionManager() override {
		return true;
	}
};

} // namespace duckdb
