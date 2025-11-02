#include "distributed_client.hpp"

#include <iostream>
#include <arrow/array.h>
#include <arrow/type.h>

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

DistributedClient::DistributedClient(const string &server_url) : server_url_(server_url) {
	Initialize();
}

DistributedClient &DistributedClient::GetInstance() {
	static DistributedClient instance;
	return instance;
}

void DistributedClient::Initialize() {
	// Only initialize once
	if (connected_) {
		return;
	}

	// Create Flight client and connect
	client_ = make_uniq<DistributedFlightClient>(server_url_);
	auto status = client_->Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, "Failed to connect to Flight server: " + status.ToString());
	}

	connected_ = true;
	std::cout << "✅ Distributed server client connected to " << server_url_ << std::endl;
}

unique_ptr<QueryResult> DistributedClient::ScanTable(const string &table_name, idx_t limit, idx_t offset) {
	// Use Flight client with protobuf ScanTableRequest - returns Arrow RecordBatches
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	auto status = client_->ScanTable(table_name, limit, offset, stream);

	if (!status.ok()) {
		return make_uniq<MaterializedQueryResult>(ErrorData(status.ToString()));
	}

	// Read all Arrow RecordBatches and convert to DuckDB
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
		if (!batch_with_metadata.data) {
			break; // End of stream
		}

		auto arrow_batch = batch_with_metadata.data;

		// On first batch, extract schema and create collection
		if (first_batch) {
			auto schema = arrow_batch->schema();

			// Convert Arrow schema to DuckDB types and names
			for (int i = 0; i < schema->num_fields(); i++) {
				auto field = schema->field(i);
				names.push_back(field->name());

				// Convert Arrow type to DuckDB LogicalType
				// For now, use simple type mapping
				auto arrow_type = field->type();
				if (arrow_type->id() == arrow::Type::INT32 || arrow_type->id() == arrow::Type::INT64) {
					types.push_back(LogicalType::INTEGER);
				} else if (arrow_type->id() == arrow::Type::STRING) {
					types.push_back(LogicalType::VARCHAR);
				} else {
					types.push_back(LogicalType::VARCHAR); // Fallback
				}
			}

			collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
			first_batch = false;
		}

		// Convert Arrow RecordBatch to DuckDB DataChunk
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), types);

		// Convert each column
		for (int col_idx = 0; col_idx < arrow_batch->num_columns(); col_idx++) {
			auto arrow_array = arrow_batch->column(col_idx);
			auto &duckdb_vector = chunk.data[col_idx];

			// Convert Arrow array to DuckDB vector
			for (int64_t row_idx = 0; row_idx < arrow_batch->num_rows(); row_idx++) {
				if (arrow_array->IsNull(row_idx)) {
					FlatVector::SetNull(duckdb_vector, row_idx, true);
				} else {
					// Type-specific conversion
					if (types[col_idx] == LogicalType::INTEGER) {
						auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
						FlatVector::GetData<int32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
					} else if (types[col_idx] == LogicalType::VARCHAR) {
						auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
						auto str_val = str_array->GetString(row_idx);
						FlatVector::GetData<string_t>(duckdb_vector)[row_idx] =
						    StringVector::AddString(duckdb_vector, str_val);
					}
				}
			}
		}

		chunk.SetCardinality(arrow_batch->num_rows());
		collection->Append(chunk);
	}

	// If no data was received, create empty collection
	if (!collection) {
		collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}

	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), ClientProperties());
}

bool DistributedClient::TableExists(const string &table_name) {
	// Use Flight client with protobuf TableExistsRequest
	bool exists = false;
	auto status = client_->TableExists(table_name, exists);

	if (!status.ok()) {
		return false;
	}

	return exists;
}

unique_ptr<QueryResult> DistributedClient::ExecuteSQL(const string &sql) {
	// Use Flight client with protobuf ExecuteSQLRequest
	distributed::DistributedResponse response;
	auto status = client_->ExecuteSQL(sql, response);

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
	// Use Flight client with protobuf CreateTableRequest
	distributed::DistributedResponse response;
	auto status = client_->CreateTable(create_sql, response);

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
	// Use Flight client with protobuf DropTableRequest
	distributed::DistributedResponse response;
	auto status = client_->DropTable(drop_sql, response);

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
	// Use Flight client with protobuf ExecuteSQLRequest
	return ExecuteSQL(insert_sql);
}

} // namespace duckdb
