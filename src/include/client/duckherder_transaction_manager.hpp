#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class DistributedClient;

class DuckherderTransactionManager : public DuckTransactionManager {
public:
	explicit DuckherderTransactionManager(AttachedDatabase &db);

	~DuckherderTransactionManager();

	Transaction &StartTransaction(ClientContext &context) override;

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;

	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	bool IsDuckTransactionManager() override {
		return true;
	}

private:
	// Returns the distributed client owned by this attached database's catalog.
	DistributedClient &GetClient();

	AttachedDatabase &attached_database;
	unique_ptr<DuckTransactionManager> duckdb_transaction_manager;
};

} // namespace duckdb
