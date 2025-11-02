#include "distributed_client.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "no_destructor.hpp"

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

unique_ptr<QueryResult> DistributedClient::ScanTable(const string &table_name, idx_t limit, idx_t offset) {
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

		// On first batch, extract schema and create collection
		if (first_batch) {
			auto schema = arrow_batch->schema();

			// Convert Arrow schema to DuckDB types and names.
			for (int idx = 0; idx < schema->num_fields(); ++idx) {
				auto field = schema->field(idx);
				names.emplace_back(field->name());

				// Convert Arrow type to DuckDB LogicalType.
				auto arrow_type = field->type();
				switch (arrow_type->id()) {
				case arrow::Type::INT8:
					types.emplace_back(LogicalType::TINYINT);
					break;
				case arrow::Type::INT16:
					types.emplace_back(LogicalType::SMALLINT);
					break;
				case arrow::Type::INT32:
					types.emplace_back(LogicalType::INTEGER);
					break;
				case arrow::Type::INT64:
					types.emplace_back(LogicalType::BIGINT);
					break;
				case arrow::Type::UINT8:
					types.emplace_back(LogicalType::UTINYINT);
					break;
				case arrow::Type::UINT16:
					types.emplace_back(LogicalType::USMALLINT);
					break;
				case arrow::Type::UINT32:
					types.emplace_back(LogicalType::UINTEGER);
					break;
				case arrow::Type::UINT64:
					types.emplace_back(LogicalType::UBIGINT);
					break;
				case arrow::Type::FLOAT:
					types.emplace_back(LogicalType::FLOAT);
					break;
				case arrow::Type::DOUBLE:
					types.emplace_back(LogicalType::DOUBLE);
					break;
				case arrow::Type::BOOL:
					types.emplace_back(LogicalType::BOOLEAN);
					break;
				case arrow::Type::STRING:
					types.emplace_back(LogicalType::VARCHAR);
					break;
				default:
					// Fallback to VARCHAR for unsupported types
					types.emplace_back(LogicalType::VARCHAR);
					break;
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
			
			// Convert based on type - match Arrow array type to DuckDB type.
			for (int64_t row_idx = 0; row_idx < arrow_batch->num_rows(); row_idx++) {
				if (arrow_array->IsNull(row_idx)) {
					FlatVector::SetNull(duckdb_vector, row_idx, true);
					continue;
				}

				// Type-specific conversion using correct Arrow array types
				auto &type = types[col_idx];
				if (type == LogicalType::TINYINT) {
					auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_array);
					FlatVector::GetData<int8_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::SMALLINT) {
					auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_array);
					FlatVector::GetData<int16_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::INTEGER) {
					auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
					FlatVector::GetData<int32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::BIGINT) {
					auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
					FlatVector::GetData<int64_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::UTINYINT) {
					auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_array);
					FlatVector::GetData<uint8_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::USMALLINT) {
					auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_array);
					FlatVector::GetData<uint16_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::UINTEGER) {
					auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_array);
					FlatVector::GetData<uint32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::UBIGINT) {
					auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_array);
					FlatVector::GetData<uint64_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
				} else if (type == LogicalType::FLOAT) {
					auto float_array = std::static_pointer_cast<arrow::FloatArray>(arrow_array);
					FlatVector::GetData<float>(duckdb_vector)[row_idx] = float_array->Value(row_idx);
				} else if (type == LogicalType::DOUBLE) {
					auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
					FlatVector::GetData<double>(duckdb_vector)[row_idx] = double_array->Value(row_idx);
				} else if (type == LogicalType::BOOLEAN) {
					auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
					FlatVector::GetData<bool>(duckdb_vector)[row_idx] = bool_array->Value(row_idx);
				} else if (type == LogicalType::VARCHAR) {
					auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
					auto str_val = str_array->GetString(row_idx);
					FlatVector::GetData<string_t>(duckdb_vector)[row_idx] =
					    StringVector::AddString(duckdb_vector, str_val);
				}
			}
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

unique_ptr<QueryResult> DistributedClient::InsertInto(const string &insert_sql) {
	return ExecuteSQL(insert_sql);
}

} // namespace duckdb
