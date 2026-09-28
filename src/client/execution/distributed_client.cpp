#include "client/execution/distributed_client.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"

#include <arrow/array.h>
#include <arrow/type.h>

namespace duckdb {

namespace {

unique_ptr<QueryResult> MakeErrorResult(const string &error) {
	return make_uniq<MaterializedQueryResult>(ErrorData(error));
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

string GetResponseError(const arrow::Status &status, const distributed::DistributedResponse &response) {
	if (!status.ok()) {
		return status.ToString();
	}
	return response.success() ? string() : response.error_message();
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

string GetTransactionError(const arrow::Status &status, const distributed::DistributedResponse &response,
                           distributed::TransactionAction action) {
	auto action_name = TransactionActionName(action);
	if (!status.ok()) {
		return StringUtil::Format("Remote Duckherder %s outcome is unknown after retry: %s", action_name,
		                          status.ToString());
	}
	if (response.success()) {
		return {};
	}
	bool unknown_outcome = false;
	if (response.has_transaction() && response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
		unknown_outcome = true;
	}
	if (action == distributed::TRANSACTION_ACTION_COMMIT && !response.has_transaction()) {
		unknown_outcome = true;
	}
	return unknown_outcome ? StringUtil::Format("Remote Duckherder %s outcome is unknown: %s", action_name,
	                                            response.error_message())
	                       : response.error_message();
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

DistributedClient::DistributedClient(string server_url_p, distributed::ClientRole role_p, DatabaseInstance &db_instance)
    : server_url(std::move(server_url_p)) {
	client = make_uniq<DistributedFlightClient>(server_url, role_p, db_instance);
	auto status = client->Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, "Failed to connect to Flight server: " + status.ToString());
	}
}

void DistributedClient::Close() {
	DistributedClientLock lock(*this);
	if (closed) {
		return;
	}
	client->Close();
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
		return MakeErrorResult(status.ToString());
	}

	// Read all Arrow RecordBatches and convert to DuckDB
	// TODO: Use DuckDB's built-in Arrow converter for better type support.
	vector<string> names;
	vector<LogicalType> types;
	unique_ptr<ColumnDataCollection> collection;
	bool first_batch = true;

	for (auto &arrow_batch : batches) {
		// On first batch, extract schema and create collection.
		if (first_batch) {
			auto schema = arrow_batch->schema();

			// If expected_types are provided, use them instead of deriving from Arrow schema.
			// This is useful to handle types like ENUM that need proper type information.
			if (expected_types != nullptr) {
				types = *expected_types;
			}

			// Convert Arrow schema to DuckDB types and names.
			for (int idx = 0; idx < schema->num_fields(); ++idx) {
				auto field = schema->field(idx);
				names.emplace_back(field->name());
				if (expected_types == nullptr) {
					types.emplace_back(ArrowTypeToDuckDBType(field->type()));
				}
			}

			collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
			first_batch = false;
		}

		// Convert Arrow RecordBatch to DuckDB DataChunk.
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), types);

		for (int col_idx = 0; col_idx < arrow_batch->num_columns(); ++col_idx) {
			auto arrow_array = arrow_batch->column(col_idx);
			auto &duckdb_vector = chunk.data[col_idx];
			ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, types[col_idx], arrow_batch->num_rows());
		}

		chunk.SetCardinality(arrow_batch->num_rows());
		collection->Append(chunk);
	}

	if (collection == nullptr) {
		// DuckDB initializes an empty result from the bound schema before execution. Flight can return no batches for
		// an empty relation, so retain the caller's bound types instead of constructing an invalid zero-column result.
		if (expected_types != nullptr) {
			types = *expected_types;
			// The table scan's output names come from its binding; placeholders keep this internal result schema
			// aligned.
			names.resize(types.size());
		}
		collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
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
                                                            const string &client_catalog) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).ExecuteStatement(sql, client_catalog, response);
	auto error = GetResponseError(status, response);
	if (!error.empty()) {
		return MakeErrorResult(error);
	}
	return MakeStatementResult(statement_type);
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
	if (!error.empty()) {
		return MakeErrorResult(error);
	}
	return MakeStatementResult(StatementType::TRANSACTION_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::LoadExtension(const string &extension_name, const string &repository,
                                                         const string &version) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).LoadExtension(extension_name, repository, version, response);
	auto error = GetResponseError(status, response);
	if (!error.empty()) {
		return MakeErrorResult(error);
	}
	return MakeStatementResult(StatementType::LOAD_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::GetQueryExecutionStats(vector<QueryExecutionStatsEntry> &stats_out) {
	DistributedClientLock lock(*this);
	distributed::DistributedResponse response;
	auto status = GetClient(lock).GetQueryExecutionStats(response);
	auto error = GetResponseError(status, response);
	if (!error.empty()) {
		return MakeErrorResult(error);
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
