#include "motherduck_transaction.hpp"
#include "motherduck_transaction_manager.hpp"

namespace duckdb {

MotherduckTransactionManager::MotherduckTransactionManager(AttachedDatabase &db, Catalog &catalog)
    : TransactionManager(db), catalog(catalog) {
}

MotherduckTransactionManager::~MotherduckTransactionManager() = default;

Transaction &MotherduckTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<MotherduckTransaction>(catalog, *this, context);
	auto &result = *transaction;
	lock_guard<mutex> guard(lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData MotherduckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> guard(lock);
	transactions.erase(transaction);
	return ErrorData();
}

void MotherduckTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> guard(lock);
	transactions.erase(transaction);
}

void MotherduckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw NotImplementedException("Checkpoint not implemented");
}

} // namespace duckdb
