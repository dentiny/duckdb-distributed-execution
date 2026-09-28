#include "duckherder_transaction_manager.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckherder_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DuckherderTransactionManager::DuckherderTransactionManager(AttachedDatabase &db)
    : DuckTransactionManager(db), attached_database(db),
      duckdb_transaction_manager(make_uniq<DuckTransactionManager>(db)) {
}

DuckherderTransactionManager::~DuckherderTransactionManager() = default;

DistributedClient &DuckherderTransactionManager::GetClient(ClientContext &context) {
	return attached_database.GetCatalog().Cast<DuckherderCatalog>().GetClient(context);
}

Transaction &DuckherderTransactionManager::StartTransaction(ClientContext &context) {
	auto &client = GetClient(context);
	auto &transaction = duckdb_transaction_manager->StartTransaction(context);
	if (!client.SetTransactionContext(context)) {
		duckdb_transaction_manager->RollbackTransaction(transaction);
		throw IOException("Duckherder client was closed while starting a transaction");
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
				duckdb_transaction_manager->RollbackTransaction(transaction);
				client->SetTransactionContext(nullptr);
				return ErrorData(result->GetError());
			}
		}
	} catch (std::exception &ex) {
		duckdb_transaction_manager->RollbackTransaction(transaction);
		return ErrorData(ex);
	}
	auto error = duckdb_transaction_manager->CommitTransaction(context, transaction);
	client->SetTransactionContext(nullptr);
	return error;
}

void DuckherderTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto context = transaction.context.lock();
	if (!context) {
		duckdb_transaction_manager->RollbackTransaction(transaction);
		return;
	}
	DistributedClient *client;
	try {
		client = &GetClient(*context);
	} catch (...) {
		duckdb_transaction_manager->RollbackTransaction(transaction);
		throw;
	}
	unique_ptr<QueryResult> result;
	try {
		if (client->HasActiveRemoteTransaction()) {
			result = client->RollbackTransaction();
		}
	} catch (...) {
		duckdb_transaction_manager->RollbackTransaction(transaction);
		client->SetTransactionContext(nullptr);
		throw;
	}
	duckdb_transaction_manager->RollbackTransaction(transaction);
	client->SetTransactionContext(nullptr);
	if (result && result->HasError()) {
		throw Exception(ExceptionType::TRANSACTION, result->GetError());
	}
}

void DuckherderTransactionManager::Checkpoint(ClientContext &context, bool force) {
	duckdb_transaction_manager->Checkpoint(context, force);
}

} // namespace duckdb
