#include "duckherder_transaction_manager.hpp"

#include "duckherder_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DuckherderTransactionManager::DuckherderTransactionManager(AttachedDatabase &db)
    : DuckTransactionManager(db), duckdb_transaction_manager(make_uniq<DuckTransactionManager>(db)) {
}

DuckherderTransactionManager::~DuckherderTransactionManager() = default;

Transaction &DuckherderTransactionManager::StartTransaction(ClientContext &context) {
	return duckdb_transaction_manager->StartTransaction(context);
}

ErrorData DuckherderTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	return duckdb_transaction_manager->CommitTransaction(context, transaction);
}

void DuckherderTransactionManager::RollbackTransaction(Transaction &transaction) {
	duckdb_transaction_manager->RollbackTransaction(transaction);
}

void DuckherderTransactionManager::Checkpoint(ClientContext &context, bool force) {
	duckdb_transaction_manager->Checkpoint(context, force);
}

} // namespace duckdb
