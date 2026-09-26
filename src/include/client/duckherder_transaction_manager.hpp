#pragma once

#include <mutex>

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class DistributedClient;
class QueryResult;

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

	// Execute DML through a lazily opened remote transaction associated with the
	// existing local DuckTransaction.
	unique_ptr<QueryResult> ExecuteRemote(ClientContext &context, const string &sql,
	                                      DatabaseModificationType modification);

	// Returns an active remote session for an explicit transaction. Autocommit
	// reads keep using the legacy worker-capable scan path.
	string GetRemoteSessionForScan(ClientContext &context);

	idx_t RemoteTransactionCountForTesting();

private:
	struct RemoteTransactionState {
		string session_id;
	};

	string EnsureRemoteTransaction(ClientContext &context, bool is_write, DatabaseModificationType modification = {});
	void CloseSessionNoThrow(const string &session_id);
	DistributedClient &GetClient();

	AttachedDatabase &attached_database;
	unique_ptr<DuckTransactionManager> duckdb_transaction_manager;
	std::mutex remote_transactions_mutex;
	unordered_map<Transaction *, RemoteTransactionState> remote_transactions;
};

} // namespace duckdb
