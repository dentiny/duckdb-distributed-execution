#include "arrow_utils.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <arrow/array.h>
#include <arrow/type.h>

namespace duckdb {

LogicalType ArrowTypeToDuckDBType(const std::shared_ptr<arrow::DataType> &arrow_type) {
	// TODO(hjiang): Add more type support for dates, timestamps, decimals, nested types, etc.
	switch (arrow_type->id()) {
	case arrow::Type::INT8:
		return LogicalType {LogicalTypeId::TINYINT};
	case arrow::Type::INT16:
		return LogicalType {LogicalTypeId::SMALLINT};
	case arrow::Type::INT32:
		return LogicalType {LogicalTypeId::INTEGER};
	case arrow::Type::INT64:
		return LogicalType {LogicalTypeId::BIGINT};
	case arrow::Type::UINT8:
		return LogicalType {LogicalTypeId::UTINYINT};
	case arrow::Type::UINT16:
		return LogicalType {LogicalTypeId::USMALLINT};
	case arrow::Type::UINT32:
		return LogicalType {LogicalTypeId::UINTEGER};
	case arrow::Type::UINT64:
		return LogicalType {LogicalTypeId::UBIGINT};
	case arrow::Type::FLOAT:
		return LogicalType {LogicalTypeId::FLOAT};
	case arrow::Type::DOUBLE:
		return LogicalType {LogicalTypeId::DOUBLE};
	case arrow::Type::BOOL:
		return LogicalType {LogicalTypeId::BOOLEAN};
	case arrow::Type::STRING:
		return LogicalType {LogicalTypeId::VARCHAR};
	default:
		// Fallback to VARCHAR for unsupported types.
		return LogicalType {LogicalTypeId::VARCHAR};
	}
}

void ConvertArrowArrayToDuckDBVector(const std::shared_ptr<arrow::Array> &arrow_array, Vector &duckdb_vector,
                                     const LogicalType &type, idx_t num_rows) {
	// Convert based on type - match Arrow array type to DuckDB type.
	for (idx_t row_idx = 0; row_idx < num_rows; row_idx++) {
		if (arrow_array->IsNull(row_idx)) {
			FlatVector::SetNull(duckdb_vector, row_idx, true);
			continue;
		}

		// Type-specific conversion using correct Arrow array types
		if (type == LogicalType {LogicalTypeId::TINYINT}) {
			auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_array);
			FlatVector::GetData<int8_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::SMALLINT}) {
			auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_array);
			FlatVector::GetData<int16_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::INTEGER}) {
			auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
			FlatVector::GetData<int32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::BIGINT}) {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
			FlatVector::GetData<int64_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::UTINYINT}) {
			auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_array);
			FlatVector::GetData<uint8_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::USMALLINT}) {
			auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_array);
			FlatVector::GetData<uint16_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::UINTEGER}) {
			auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_array);
			FlatVector::GetData<uint32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::UBIGINT}) {
			auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_array);
			FlatVector::GetData<uint64_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::FLOAT}) {
			auto float_array = std::static_pointer_cast<arrow::FloatArray>(arrow_array);
			FlatVector::GetData<float>(duckdb_vector)[row_idx] = float_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::DOUBLE}) {
			auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
			FlatVector::GetData<double>(duckdb_vector)[row_idx] = double_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::BOOLEAN}) {
			auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
			FlatVector::GetData<bool>(duckdb_vector)[row_idx] = bool_array->Value(row_idx);
		} else if (type == LogicalType {LogicalTypeId::VARCHAR}) {
			auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
			auto str_val = str_array->GetString(row_idx);
			FlatVector::GetData<string_t>(duckdb_vector)[row_idx] = StringVector::AddString(duckdb_vector, str_val);
		}
	}
}

} // namespace duckdb

