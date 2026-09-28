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

unique_ptr<QueryResult> MakeEmptyResult(StatementType statement_type) {
	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(statement_type, StatementProperties(), names, std::move(collection),
	                                          ClientProperties());
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

DistributedClient::DistributedClient(string server_url_p, distributed::ClientRole role_p, DatabaseInstance &db_instance)
    : server_url(std::move(server_url_p)) {
	client = make_uniq<DistributedFlightClient>(server_url, role_p, db_instance);
	auto status = client->Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, "Failed to connect to Flight server: " + status.ToString());
	}
}

void DistributedClient::Close() {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return;
	}
	client->Close();
	closed = true;
}

bool DistributedClient::SetTransactionContext(optional_ptr<ClientContext> context) {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return false;
	}
	client->SetTransactionContext(context);
	return true;
}

bool DistributedClient::HasActiveRemoteTransaction() {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		throw IOException("Duckherder client is closed");
	}
	return client->HasActiveTransaction();
}

unique_ptr<QueryResult> DistributedClient::ScanTable(const string &table_name, idx_t limit, idx_t offset,
                                                     const vector<LogicalType> *expected_types) {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return MakeErrorResult("Duckherder client is closed");
	}
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto status = client->ScanTable(table_name, limit, offset, batches);
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
		collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

bool DistributedClient::TableExists(const string &table_name) {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		throw IOException("Duckherder client is closed");
	}
	bool exists = false;
	auto status = client->TableExists(table_name, exists);
	if (!status.ok()) {
		throw IOException("Failed to check remote table existence: %s", status.ToString());
	}
	return exists;
}

unique_ptr<QueryResult> DistributedClient::ExecuteStatement(const string &sql, const string &client_catalog) {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return MakeErrorResult("Duckherder client is closed");
	}
	distributed::DistributedResponse response;
	auto status = client->ExecuteStatement(sql, client_catalog, response);
	auto error = GetResponseError(status, response);
	if (!error.empty()) {
		return MakeErrorResult(error);
	}
	return MakeEmptyResult(StatementType::INSERT_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::CommitTransaction() {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return MakeErrorResult("Duckherder client is closed");
	}
	return ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT);
}

unique_ptr<QueryResult> DistributedClient::RollbackTransaction() {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return MakeErrorResult("Duckherder client is closed");
	}
	return ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK);
}

unique_ptr<QueryResult> DistributedClient::ManageTransaction(distributed::TransactionAction action) {
	distributed::DistributedResponse response;
	auto status = client->ManageTransaction(action, response);
	auto error = GetTransactionError(status, response, action);
	if (!error.empty()) {
		return MakeErrorResult(error);
	}
	return MakeEmptyResult(StatementType::TRANSACTION_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::LoadExtension(const string &extension_name, const string &repository,
                                                         const string &version) {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return MakeErrorResult("Duckherder client is closed");
	}
	distributed::DistributedResponse response;
	auto status = client->LoadExtension(extension_name, repository, version, response);
	auto error = GetResponseError(status, response);
	if (!error.empty()) {
		return MakeErrorResult(error);
	}
	return MakeEmptyResult(StatementType::LOAD_STATEMENT);
}

unique_ptr<QueryResult> DistributedClient::GetQueryExecutionStats(vector<QueryExecutionStatsEntry> &stats_out) {
	const concurrency::lock_guard<concurrency::mutex> lock(lifecycle_mutex);
	if (closed) {
		return MakeErrorResult("Duckherder client is closed");
	}
	distributed::DistributedResponse response;
	auto status = client->GetQueryExecutionStats(response);
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

	return MakeEmptyResult(StatementType::SELECT_STATEMENT);
}

} // namespace duckdb
