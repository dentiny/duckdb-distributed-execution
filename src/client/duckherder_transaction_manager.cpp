#include "duckherder_transaction_manager.hpp"

#include "distributed_client.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

DuckherderTransactionManager::DuckherderTransactionManager(AttachedDatabase &db)
    : DuckTransactionManager(db), attached_database(db),
      duckdb_transaction_manager(make_uniq<DuckTransactionManager>(db)) {
}

DuckherderTransactionManager::~DuckherderTransactionManager() {
	vector<string> session_ids;
	{
		const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
		for (auto &entry : remote_transactions) {
			session_ids.push_back(entry.second.session_id);
		}
		remote_transactions.clear();
	}
	for (auto &session_id : session_ids) {
		CloseSessionNoThrow(session_id);
	}
}

Transaction &DuckherderTransactionManager::StartTransaction(ClientContext &context) {
	return duckdb_transaction_manager->StartTransaction(context);
}

ErrorData DuckherderTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	string session_id;
	{
		const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
		auto entry = remote_transactions.find(&transaction);
		if (entry != remote_transactions.end()) {
			session_id = entry->second.session_id;
		}
	}

	if (!session_id.empty()) {
		auto result = GetClient().ExecuteSQL("COMMIT", session_id);
		if (result->HasError()) {
			{
				const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
				remote_transactions.erase(&transaction);
			}
			// The Flight client already retried COMMIT using the stable session
			// identifier. A remaining transport error means authoritative status
			// could not be recovered. Remove the pointer-keyed bookkeeping entry
			// before DuckDB can reuse the transaction address; session close/TTL
			// owns any remaining stable server-side state.
			duckdb_transaction_manager->RollbackTransaction(transaction);
			CloseSessionNoThrow(session_id);
			return ErrorData(ExceptionType::TRANSACTION,
			                 "Remote Duckherder COMMIT outcome is unknown: " + result->GetError());
		}
		const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
		remote_transactions.erase(&transaction);
	}

	auto error = duckdb_transaction_manager->CommitTransaction(context, transaction);
	if (!session_id.empty()) {
		CloseSessionNoThrow(session_id);
	}
	return error;
}

void DuckherderTransactionManager::RollbackTransaction(Transaction &transaction) {
	string session_id;
	{
		const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
		auto entry = remote_transactions.find(&transaction);
		if (entry != remote_transactions.end()) {
			session_id = entry->second.session_id;
			remote_transactions.erase(entry);
		}
	}

	string remote_error;
	if (!session_id.empty()) {
		auto result = GetClient().ExecuteSQL("ROLLBACK", session_id);
		if (result->HasError()) {
			remote_error = result->GetError();
		}
	}
	duckdb_transaction_manager->RollbackTransaction(transaction);
	if (!session_id.empty()) {
		CloseSessionNoThrow(session_id);
	}
	if (!remote_error.empty()) {
		throw IOException("Failed to roll back remote Duckherder transaction: %s", remote_error);
	}
}

void DuckherderTransactionManager::Checkpoint(ClientContext &context, bool force) {
	duckdb_transaction_manager->Checkpoint(context, force);
}

DistributedClient &DuckherderTransactionManager::GetClient() {
	return attached_database.GetCatalog().Cast<DuckherderCatalog>().GetClient();
}

void DuckherderTransactionManager::CloseSessionNoThrow(const string &session_id) {
	try {
		GetClient().CloseSession(session_id);
	} catch (...) {
		// Session close is best-effort after transaction completion. The server also
		// rolls back active transactions when a connection is closed.
	}
}

string DuckherderTransactionManager::EnsureRemoteTransaction(ClientContext &context, bool is_write,
                                                             DatabaseModificationType modification) {
	auto &meta_transaction = MetaTransaction::Get(context);
	if (is_write) {
		meta_transaction.ModifyDatabase(attached_database, modification);
	}
	auto &transaction = meta_transaction.GetTransaction(attached_database);

	const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
	auto entry = remote_transactions.find(&transaction);
	if (entry != remote_transactions.end()) {
		return entry->second.session_id;
	}

	auto session_id = GetClient().OpenSession();
	auto begin_result = GetClient().ExecuteSQL("BEGIN TRANSACTION", session_id);
	if (begin_result->HasError()) {
		CloseSessionNoThrow(session_id);
		throw TransactionException("Failed to begin remote Duckherder transaction: %s", begin_result->GetError());
	}
	remote_transactions.emplace(&transaction, RemoteTransactionState {session_id});
	return session_id;
}

unique_ptr<QueryResult> DuckherderTransactionManager::ExecuteRemote(ClientContext &context, const string &sql,
                                                                    DatabaseModificationType modification) {
	auto session_id = EnsureRemoteTransaction(context, true, modification);
	return GetClient().ExecuteSQL(sql, session_id);
}

string DuckherderTransactionManager::GetRemoteSessionForScan(ClientContext &context) {
	if (context.transaction.IsAutoCommit()) {
		return "";
	}
	return EnsureRemoteTransaction(context, false);
}

idx_t DuckherderTransactionManager::RemoteTransactionCountForTesting() {
	const std::lock_guard<std::mutex> lock(remote_transactions_mutex);
	return remote_transactions.size();
}

} // namespace duckdb
