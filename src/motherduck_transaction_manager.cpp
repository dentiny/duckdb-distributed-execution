#include "motherduck_transaction_manager.hpp"

#include "motherduck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

MotherduckTransactionManager::MotherduckTransactionManager(AttachedDatabase &db)
    : TransactionManager(db), duckdb_transaction_manager(make_uniq<DuckTransactionManager>(db)) {
}

MotherduckTransactionManager::~MotherduckTransactionManager() = default;

Transaction &MotherduckTransactionManager::StartTransaction(ClientContext &context) {
	return duckdb_transaction_manager->StartTransaction(context);
}

ErrorData MotherduckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	return duckdb_transaction_manager->CommitTransaction(context, transaction);
}

void MotherduckTransactionManager::RollbackTransaction(Transaction &transaction) {
	duckdb_transaction_manager->RollbackTransaction(transaction);
}

void MotherduckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	duckdb_transaction_manager->Checkpoint(context, force);
}

} // namespace duckdb
