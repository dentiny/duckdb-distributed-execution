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

DistributedClient::DistributedClient(string server_url_p, distributed::ClientRole role_p, DatabaseInstance &db_instance)
    : server_url(std::move(server_url_p)) {
	client = make_uniq<DistributedFlightClient>(server_url, role_p, db_instance);
	auto status = client->Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, "Failed to connect to Flight server: " + status.ToString());
	}
}

void DistributedClient::Close() {
	client->Close();
}

unique_ptr<QueryResult> DistributedClient::ScanTable(const string &table_name, idx_t limit, idx_t offset,
                                                     const vector<LogicalType> *expected_types) {
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto status = client->ScanTable(table_name, limit, offset, batches);
	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
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
	bool exists = false;
	auto status = client->TableExists(table_name, exists);
	if (!status.ok()) {
		return false;
	}
	return exists;
}

unique_ptr<QueryResult> DistributedClient::ExecuteStatement(const string &sql, const string &client_catalog) {
	distributed::DistributedResponse response;
	auto status = client->ExecuteStatement(sql, client_catalog, response);
	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}
	if (!response.success()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(response.error_message()));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::INSERT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::BeginTransaction() {
	return ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN);
}

unique_ptr<QueryResult> DistributedClient::CommitTransaction() {
	return ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT);
}

unique_ptr<QueryResult> DistributedClient::RollbackTransaction() {
	return ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK);
}

unique_ptr<QueryResult> DistributedClient::ManageTransaction(distributed::TransactionAction action) {
	distributed::DistributedResponse response;
	auto status = client->ManageTransaction(action, response);
	if (!status.ok()) {
		auto error = status.ToString();
		if (action == distributed::TRANSACTION_ACTION_COMMIT) {
			error = StringUtil::Format("Remote Duckherder COMMIT outcome is unknown after retry: %s", error);
		}
		return make_uniq<MaterializedQueryResult>(ErrorData(error));
	}
	if (!response.success()) {
		auto error = response.error_message();
		if (action == distributed::TRANSACTION_ACTION_COMMIT &&
		    (!response.has_transaction() ||
		     response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN)) {
			error = StringUtil::Format("Remote Duckherder COMMIT outcome is unknown: %s", error);
		}
		return make_uniq<MaterializedQueryResult>(ErrorData(error));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::TRANSACTION_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::LoadExtension(const string &extension_name, const string &repository,
                                                         const string &version) {
	distributed::DistributedResponse transaction_response;
	auto transaction_status = client->ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, transaction_response);
	if (!transaction_status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(transaction_status.ToString()));
	}
	if (!transaction_response.success()) {
		if (transaction_response.has_transaction() &&
		    transaction_response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
			return make_uniq<MaterializedQueryResult>(ErrorData(StringUtil::Format(
			    "Remote extension transaction BEGIN outcome is unknown: %s", transaction_response.error_message())));
		}
		return make_uniq<MaterializedQueryResult>(ErrorData(transaction_response.error_message()));
	}
	auto rollback_after_error = [&](const string &operation_error) -> unique_ptr<QueryResult> {
		distributed::DistributedResponse rollback_response;
		auto rollback_status = client->ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, rollback_response);
		if (!rollback_status.ok()) {
			return make_uniq<MaterializedQueryResult>(ErrorData(StringUtil::Format(
			    "%s; remote rollback outcome is unknown: %s", operation_error, rollback_status.ToString())));
		}
		if (!rollback_response.success()) {
			return make_uniq<MaterializedQueryResult>(ErrorData(StringUtil::Format(
			    "%s; remote rollback failed: %s", operation_error, rollback_response.error_message())));
		}
		return make_uniq<MaterializedQueryResult>(ErrorData(operation_error));
	};

	distributed::DistributedResponse response;
	auto status = client->LoadExtension(extension_name, repository, version, response);
	if (!status.ok()) {
		return rollback_after_error(status.ToString());
	}
	if (!response.success()) {
		return rollback_after_error(response.error_message());
	}
	transaction_status = client->ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, transaction_response);
	if (!transaction_status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(StringUtil::Format(
		    "Remote extension transaction COMMIT outcome is unknown: %s", transaction_status.ToString())));
	}
	if (!transaction_response.success()) {
		if (transaction_response.has_transaction() &&
		    transaction_response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN) {
			return make_uniq<MaterializedQueryResult>(ErrorData(StringUtil::Format(
			    "Remote extension transaction COMMIT outcome is unknown: %s", transaction_response.error_message())));
		}
		return make_uniq<MaterializedQueryResult>(ErrorData(transaction_response.error_message()));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::LOAD_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::GetQueryExecutionStats(vector<QueryExecutionStatsEntry> &stats_out) {
	distributed::DistributedResponse response;
	auto status = client->GetQueryExecutionStats(response);

	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}
	if (!response.success()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(response.error_message()));
	}

	// Extract stats from the response
	const auto &stats_response = response.get_query_execution_stats();
	stats_out.clear();
	stats_out.reserve(stats_response.query_executions_size());

	for (int idx = 0; idx < stats_response.query_executions_size(); ++idx) {
		stats_out.emplace_back(stats_response.query_executions(idx));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

} // namespace duckdb
