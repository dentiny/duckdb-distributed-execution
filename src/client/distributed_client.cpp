#include "distributed_client.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "utils/no_destructor.hpp"

#include <arrow/array.h>
#include <arrow/type.h>

namespace duckdb {

DistributedClient::DistributedClient(string server_url_p) : server_url(std::move(server_url_p)) {
	client = make_uniq<DistributedFlightClient>(server_url);
	auto status = client->Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, "Failed to connect to Flight server: " + status.ToString());
	}
}

DistributedClient &DistributedClient::GetInstance() {
	static NoDestructor<DistributedClient> client {};
	return *client;
}

unique_ptr<QueryResult> DistributedClient::ScanTable(const string &table_name, idx_t limit, idx_t offset,
                                                     const vector<LogicalType> *expected_types) {
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	auto status = client->ScanTable(table_name, limit, offset, stream);
	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}

	// Read all Arrow RecordBatches and convert to DuckDB
	// TODO: Use DuckDB's built-in Arrow converter for better type support.
	vector<string> names;
	vector<LogicalType> types;
	unique_ptr<ColumnDataCollection> collection;
	bool first_batch = true;

	while (true) {
		auto result = stream->Next();
		if (!result.ok()) {
			return make_uniq<MaterializedQueryResult>(ErrorData(result.status().ToString()));
		}

		auto batch_with_metadata = result.ValueOrDie();
		auto arrow_batch = std::move(batch_with_metadata.data);
		if (arrow_batch == nullptr) {
			break; // End of stream
		}

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

unique_ptr<QueryResult> DistributedClient::ExecuteSQL(const string &sql) {
	distributed::DistributedResponse response;
	auto status = client->ExecuteSQL(sql, response);

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

unique_ptr<QueryResult> DistributedClient::CreateTable(const string &create_sql) {
	distributed::DistributedResponse response;
	auto status = client->CreateTable(create_sql, response);

	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}
	if (!response.success()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(response.error_message()));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::CREATE_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::DropTable(const string &drop_sql) {
	distributed::DistributedResponse response;
	auto status = client->DropTable(drop_sql, response);

	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}
	if (!response.success()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(response.error_message()));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::DROP_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::CreateIndex(const string &create_sql) {
	distributed::DistributedResponse response;
	auto status = client->CreateIndex(create_sql, response);

	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}
	if (!response.success()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(response.error_message()));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::CREATE_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::DropIndex(const string &index_name) {
	distributed::DistributedResponse response;
	auto status = client->DropIndex(index_name, response);

	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}
	if (!response.success()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(response.error_message()));
	}

	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	return make_uniq<MaterializedQueryResult>(StatementType::DROP_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

unique_ptr<QueryResult> DistributedClient::InsertInto(const string &insert_sql) {
	return ExecuteSQL(insert_sql);
}

} // namespace duckdb
