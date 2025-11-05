#include "duckling_transaction_manager.hpp"

#include "duckling_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DucklingTransactionManager::DucklingTransactionManager(AttachedDatabase &db)
    : DuckTransactionManager(db), duckdb_transaction_manager(make_uniq<DuckTransactionManager>(db)) {
}

DucklingTransactionManager::~DucklingTransactionManager() = default;

Transaction &DucklingTransactionManager::StartTransaction(ClientContext &context) {
	return duckdb_transaction_manager->StartTransaction(context);
}

ErrorData DucklingTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	return duckdb_transaction_manager->CommitTransaction(context, transaction);
}

void DucklingTransactionManager::RollbackTransaction(Transaction &transaction) {
	duckdb_transaction_manager->RollbackTransaction(transaction);
}

void DucklingTransactionManager::Checkpoint(ClientContext &context, bool force) {
	duckdb_transaction_manager->Checkpoint(context, force);
}

} // namespace duckdb

