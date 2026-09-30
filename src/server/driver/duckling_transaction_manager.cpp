#include "server/driver/duckling_transaction_manager.hpp"

#include "server/driver/duckling_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DucklingTransactionManager::DucklingTransactionManager(AttachedDatabase &db) : DuckTransactionManager(db) {
}

DucklingTransactionManager::~DucklingTransactionManager() = default;

Transaction &DucklingTransactionManager::StartTransaction(ClientContext &context) {
	return DuckTransactionManager::StartTransaction(context);
}

ErrorData DucklingTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	return DuckTransactionManager::CommitTransaction(context, transaction);
}

void DucklingTransactionManager::RollbackTransaction(Transaction &transaction) {
	DuckTransactionManager::RollbackTransaction(transaction);
}

void DucklingTransactionManager::Checkpoint(ClientContext &context, bool force) {
	DuckTransactionManager::Checkpoint(context, force);
}

} // namespace duckdb
