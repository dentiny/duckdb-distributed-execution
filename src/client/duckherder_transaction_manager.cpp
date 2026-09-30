#include "duckherder_transaction_manager.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckherder_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DuckherderTransactionManager::DuckherderTransactionManager(AttachedDatabase &db)
    : DuckTransactionManager(db), attached_database(db) {
}

DuckherderTransactionManager::~DuckherderTransactionManager() = default;

DistributedClient &DuckherderTransactionManager::GetClient(ClientContext &context) {
	return attached_database.GetCatalog().Cast<DuckherderCatalog>().GetClient(context);
}

Transaction &DuckherderTransactionManager::StartTransaction(ClientContext &context) {
	auto &client = GetClient(context);
	auto &transaction = DuckTransactionManager::StartTransaction(context);
	try {
		client.SetTransactionContext(context);
	} catch (...) {
		DuckTransactionManager::RollbackTransaction(transaction);
		throw;
	}
	return transaction;
}

ErrorData DuckherderTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	DistributedClient *client;
	try {
		client = &GetClient(context);
		if (client->HasActiveRemoteTransaction()) {
			auto result = client->CommitTransaction();
			if (result->HasError()) {
				DuckTransactionManager::RollbackTransaction(transaction);
				client->ClearTransactionContext();
				return result->GetErrorObject();
			}
		}
	} catch (std::exception &ex) {
		DuckTransactionManager::RollbackTransaction(transaction);
		return ErrorData(ex);
	}
	auto error = DuckTransactionManager::CommitTransaction(context, transaction);
	client->ClearTransactionContext();
	return error;
}

void DuckherderTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto context = transaction.context.lock();
	if (!context) {
		DuckTransactionManager::RollbackTransaction(transaction);
		return;
	}
	DistributedClient *client;
	try {
		client = &GetClient(*context);
	} catch (...) {
		DuckTransactionManager::RollbackTransaction(transaction);
		throw;
	}
	unique_ptr<QueryResult> result;
	try {
		if (client->HasActiveRemoteTransaction()) {
			result = client->RollbackTransaction();
		}
	} catch (...) {
		DuckTransactionManager::RollbackTransaction(transaction);
		client->ClearTransactionContext();
		throw;
	}
	DuckTransactionManager::RollbackTransaction(transaction);
	client->ClearTransactionContext();
	if (result && result->HasError()) {
		throw Exception(ExceptionType::TRANSACTION, result->GetError());
	}
}

void DuckherderTransactionManager::Checkpoint(ClientContext &context, bool force) {
	DuckTransactionManager::Checkpoint(context, force);
}

} // namespace duckdb
