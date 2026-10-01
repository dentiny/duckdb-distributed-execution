#include "client/execution/distributed_client.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "utils/remote_error.hpp"

#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/type.h>

namespace duckdb {

namespace {

unique_ptr<QueryResult> MakeErrorResult(ErrorData error) {
	return make_uniq<MaterializedQueryResult>(std::move(error));
}

unique_ptr<QueryResult> MakeErrorResult(const string &error) {
	return MakeErrorResult(ErrorData(error));
}

ErrorData GetResponseError(const arrow::Status &status, const distributed::DistributedResponse &response) {
	if (!status.ok()) {
		return ErrorData(ExceptionType::IO, status.ToString());
	}
	if (response.success()) {
		return ErrorData();
	}
	return response.has_error() ? FromRemoteError(response.error()) : ErrorData(response.error_message());
}

unique_ptr<QueryResult> MakeEmptyResult(StatementType statement_type, string name, LogicalType type) {
	vector<string> names {std::move(name)};
	vector<LogicalType> types {std::move(type)};
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(statement_type, StatementProperties(), names, std::move(collection),
	                                          ClientProperties());
}

// Match DuckDB's binder-defined result schemas, when these remote operations return no result rows.
unique_ptr<QueryResult> MakeStatementResult(StatementType statement_type) {
	switch (statement_type) {
	case StatementType::CREATE_STATEMENT:
	case StatementType::INSERT_STATEMENT:
	case StatementType::DELETE_STATEMENT:
	case StatementType::UPDATE_STATEMENT:
	case StatementType::MERGE_INTO_STATEMENT:
		return MakeEmptyResult(statement_type, "Count", LogicalType::BIGINT);
	case StatementType::ALTER_STATEMENT:
	case StatementType::DROP_STATEMENT:
	case StatementType::TRANSACTION_STATEMENT:
	case StatementType::LOAD_STATEMENT:
		return MakeEmptyResult(statement_type, "Success", LogicalType::BOOLEAN);
	default:
		throw InternalException("Unsupported remote statement result type");
	}
}

const char *TransactionActionName(distributed::TransactionAction action) {
	switch (action) {
	case distributed::TRANSACTION_ACTION_BEGIN:
		return "BEGIN";
	case distributed::TRANSACTION_ACTION_COMMIT:
		return "COMMIT";
	case distributed::TRANSACTION_ACTION_ROLLBACK:
		return "ROLLBACK";
	default:
		return "transaction";
	}
}

ErrorData GetTransactionError(const arrow::Status &status, const distributed::DistributedResponse &response,
                              distributed::TransactionAction action) {
	auto action_name = TransactionActionName(action);
	if (!status.ok()) {
		return ErrorData(StringUtil::Format("Remote Duckherder %s outcome is unknown after retry: %s", action_name,
		                                    status.ToString()));
	}
	if (response.success()) {
		return ErrorData();
	}
	bool unknown_outcome = false;
	if (response.has_transaction() && response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
		unknown_outcome = true;
	}
	if (action == distributed::TRANSACTION_ACTION_COMMIT && !response.has_transaction()) {
		unknown_outcome = true;
	}
	if (unknown_outcome) {
		return ErrorData(
		    StringUtil::Format("Remote Duckherder %s outcome is unknown: %s", action_name, response.error_message()));
	}
	return response.has_error() ? FromRemoteError(response.error()) : ErrorData(response.error_message());
}

} // namespace

DistributedClient::DistributedClientLock::DistributedClientLock(DistributedClient &owner_p)
    : guard(owner_p.lifecycle_mutex) {
}

DistributedClient::DistributedClientLock::~DistributedClientLock() = default;

DistributedFlightClient &DistributedClient::GetClient(DistributedClientLock &) {
	if (closed || !client) {
		throw IOException("Duckherder client is closed");
	}
	return *client;
}

ClientContext &DistributedClient::GetArrowContext(DistributedClientLock &) {
	if (!arrow_connection) {
		arrow_connection = make_uniq<Connection>(db_instance);
	}
	return *arrow_connection->context;
}

DistributedClient::DistributedClient(string server_url_p, distributed::ClientRole role_p, DatabaseInstance &db_instance,
                                     distributed::StorageConfig storage_config)
    : server_url(std::move(server_url_p)), db_instance(db_instance) {
	client = make_uniq<DistributedFlightClient>(server_url, role_p, db_instance, std::move(storage_config));
	auto status = client->Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION,
		                StringUtil::Format("Failed to connect to Flight server: %s", status.ToString()));
	}
}

void DistributedClient::Close() {
	DistributedClientLock lock(*this);
	if (closed) {
		return;
	}
	client->Close();
	arrow_connection.reset();
	closed = true;
}

void DistributedClient::SetTransactionContext(ClientContext &context) {
	DistributedClientLock lock(*this);
	GetClient(lock).SetTransactionContext(context);
}

void DistributedClient::ClearTransactionContext() {
	DistributedClientLock lock(*this);
	if (!closed) {
		client->SetTransactionContext(nullptr);
	}
}

bool DistributedClient::HasActiveRemoteTransaction() {
	DistributedClientLock lock(*this);
	return GetClient(lock).HasActiveTransaction();
}

unique_ptr<QueryResult> DistributedClient::ScanTable(const string &table_name, idx_t limit, idx_t offset,
                                                     const vector<LogicalType> *expected_types) {
	DistributedClientLock lock(*this);
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto status = GetClient(lock).ScanTable(table_name, limit, offset, batches);
	if (!status.ok()) {
		return MakeErrorResult(ErrorData(ExceptionType::IO, status.ToString()));
	}
	auto schema = batches.empty() ? nullptr : batches[0]->schema();
	return MakeArrowResult(GetArrowContext(lock), StatementType::SELECT_STATEMENT, batches, schema, expected_types);
}

bool DistributedClient::TableExists(const string &table_name) {
	DistributedClientLock lock(*this);
	bool exists = false;
	auto status = GetClient(lock).TableExists(table_name, exists);
	if (!status.ok()) {
		throw IOException("Failed to check remote table existence: %s", status.ToString());
	}
	return exists;
}

unique_ptr<QueryResult> DistributedClient::ExecuteStatement(const string &sql, StatementType statement_type,
                                                            const string &client_catalog,
                                                            const vector<LogicalType> *expected_types) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).ExecuteStatement(sql, client_catalog, response);
	auto error = GetResponseError(status, response);
	if (error.HasError()) {
		return MakeErrorResult(std::move(error));
	}
	const auto &ipc_result = response.execute_statement().arrow_ipc_result();
	if (ipc_result.empty()) {
		return MakeStatementResult(statement_type);
	}

	auto buffer =
	    std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(ipc_result.data()), ipc_result.size());
	auto input = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		return MakeErrorResult(reader_result.status().ToString());
	}
	auto reader = reader_result.ValueOrDie();
	auto batches_result = reader->ToRecordBatches();
	if (!batches_result.ok()) {
		return MakeErrorResult(batches_result.status().ToString());
	}
	auto arrow_batches = batches_result.ValueOrDie();
	vector<std::shared_ptr<arrow::RecordBatch>> batches(arrow_batches.begin(), arrow_batches.end());
	return MakeArrowResult(GetArrowContext(lock), statement_type, batches, reader->schema(), expected_types);
}

unique_ptr<QueryResult> DistributedClient::CommitTransaction() {
	return ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT);
}

unique_ptr<QueryResult> DistributedClient::RollbackTransaction() {
	return ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK);
}

unique_ptr<QueryResult> DistributedClient::ManageTransaction(distributed::TransactionAction action) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).ManageTransaction(action, response);
	auto error = GetTransactionError(status, response, action);
	if (error.HasError()) {
		return MakeErrorResult(std::move(error));
	}
	return MakeStatementResult(StatementType::TRANSACTION_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::LoadExtension(const string &extension_name, const string &repository,
                                                         const string &version) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).LoadExtension(extension_name, repository, version, response);
	auto error = GetResponseError(status, response);
	if (error.HasError()) {
		return MakeErrorResult(std::move(error));
	}
	return MakeStatementResult(StatementType::LOAD_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::GetQueryExecutionStats(vector<QueryExecutionStatsEntry> &stats_out) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).GetQueryExecutionStats(response);
	auto error = GetResponseError(status, response);
	if (error.HasError()) {
		return MakeErrorResult(std::move(error));
	}

	// Extract stats from the response
	const auto &stats_response = response.get_query_execution_stats();
	stats_out.clear();
	stats_out.reserve(stats_response.query_executions_size());

	for (int idx = 0; idx < stats_response.query_executions_size(); ++idx) {
		stats_out.emplace_back(stats_response.query_executions(idx));
	}

	return MakeEmptyResult(StatementType::SELECT_STATEMENT, "Success", LogicalType::BOOLEAN);
}

} // namespace duckdb
