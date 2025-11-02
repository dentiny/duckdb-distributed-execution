#include "arrow_utils.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <arrow/array.h>
#include <arrow/type.h>
#include <cstring>

namespace duckdb {

LogicalType ArrowTypeToDuckDBType(const std::shared_ptr<arrow::DataType> &arrow_type) {
	// TODO: Add support for complex nested types (LIST, STRUCT, MAP) and more temporal variants.
	switch (arrow_type->id()) {
	case arrow::Type::NA:
		return LogicalType {LogicalTypeId::SQLNULL};
	case arrow::Type::BOOL:
		return LogicalType {LogicalTypeId::BOOLEAN};
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
	case arrow::Type::HALF_FLOAT:
		// Half-precision floats convert to regular float.
		return LogicalType {LogicalTypeId::FLOAT};
	case arrow::Type::FLOAT:
		return LogicalType {LogicalTypeId::FLOAT};
	case arrow::Type::DOUBLE:
		return LogicalType {LogicalTypeId::DOUBLE};
	case arrow::Type::STRING:
	case arrow::Type::LARGE_STRING:
		return LogicalType {LogicalTypeId::VARCHAR};
	case arrow::Type::BINARY:
	case arrow::Type::LARGE_BINARY:
	case arrow::Type::FIXED_SIZE_BINARY:
		return LogicalType {LogicalTypeId::BLOB};
	case arrow::Type::DATE32:
	case arrow::Type::DATE64:
		return LogicalType {LogicalTypeId::DATE};
	case arrow::Type::TIME32:
	case arrow::Type::TIME64:
		return LogicalType {LogicalTypeId::TIME};
	case arrow::Type::TIMESTAMP:
		return LogicalType {LogicalTypeId::TIMESTAMP};
	case arrow::Type::INTERVAL_MONTHS:
	case arrow::Type::INTERVAL_DAY_TIME:
	case arrow::Type::INTERVAL_MONTH_DAY_NANO:
		return LogicalType {LogicalTypeId::INTERVAL};
	case arrow::Type::DECIMAL128:
	case arrow::Type::DECIMAL256: {
		// Extract precision and scale from Arrow decimal type.
		auto decimal_type = std::static_pointer_cast<arrow::DecimalType>(arrow_type);
		return LogicalType::DECIMAL(decimal_type->precision(), decimal_type->scale());
	}
	default:
		// Fallback to VARCHAR for unsupported types (LIST, STRUCT, MAP, etc.).
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

		// Type-specific conversion using correct Arrow array types.
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
			// NULL values are already handled above.
			FlatVector::SetNull(duckdb_vector, row_idx, true);
			break;
		case LogicalTypeId::BOOLEAN: {
			auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
			FlatVector::GetData<bool>(duckdb_vector)[row_idx] = bool_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::TINYINT: {
			auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_array);
			FlatVector::GetData<int8_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_array);
			FlatVector::GetData<int16_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::INTEGER: {
			auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
			FlatVector::GetData<int32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::BIGINT: {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
			FlatVector::GetData<int64_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::UTINYINT: {
			auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_array);
			FlatVector::GetData<uint8_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::USMALLINT: {
			auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_array);
			FlatVector::GetData<uint16_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::UINTEGER: {
			auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_array);
			FlatVector::GetData<uint32_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::UBIGINT: {
			auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_array);
			FlatVector::GetData<uint64_t>(duckdb_vector)[row_idx] = int_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::FLOAT: {
			// Handle both HALF_FLOAT and FLOAT Arrow types.
			if (arrow_array->type_id() == arrow::Type::HALF_FLOAT) {
				auto half_array = std::static_pointer_cast<arrow::HalfFloatArray>(arrow_array);
				// Convert half float to regular float.
				FlatVector::GetData<float>(duckdb_vector)[row_idx] = static_cast<float>(half_array->Value(row_idx));
			} else {
				auto float_array = std::static_pointer_cast<arrow::FloatArray>(arrow_array);
				FlatVector::GetData<float>(duckdb_vector)[row_idx] = float_array->Value(row_idx);
			}
			break;
		}
		case LogicalTypeId::DOUBLE: {
			auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
			FlatVector::GetData<double>(duckdb_vector)[row_idx] = double_array->Value(row_idx);
			break;
		}
		case LogicalTypeId::VARCHAR: {
			// Handle both STRING and LARGE_STRING.
			if (arrow_array->type_id() == arrow::Type::LARGE_STRING) {
				auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(arrow_array);
				auto str_val = str_array->GetString(row_idx);
				FlatVector::GetData<string_t>(duckdb_vector)[row_idx] = StringVector::AddString(duckdb_vector, str_val);
			} else {
				auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
				auto str_val = str_array->GetString(row_idx);
				FlatVector::GetData<string_t>(duckdb_vector)[row_idx] = StringVector::AddString(duckdb_vector, str_val);
			}
			break;
		}
		case LogicalTypeId::BLOB: {
			// Handle BINARY, LARGE_BINARY, and FIXED_SIZE_BINARY.
			string_t blob_val;
			if (arrow_array->type_id() == arrow::Type::BINARY) {
				auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(arrow_array);
				int32_t length;
				auto data = binary_array->GetValue(row_idx, &length);
				blob_val = StringVector::AddStringOrBlob(duckdb_vector,
				                                         string_t(reinterpret_cast<const char *>(data), length));
			} else if (arrow_array->type_id() == arrow::Type::LARGE_BINARY) {
				auto binary_array = std::static_pointer_cast<arrow::LargeBinaryArray>(arrow_array);
				int64_t length;
				auto data = binary_array->GetValue(row_idx, &length);
				blob_val = StringVector::AddStringOrBlob(
				    duckdb_vector, string_t(reinterpret_cast<const char *>(data), static_cast<uint32_t>(length)));
			} else { // FIXED_SIZE_BINARY
				auto binary_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_array);
				auto data = binary_array->GetValue(row_idx);
				auto length = binary_array->byte_width();
				blob_val = StringVector::AddStringOrBlob(duckdb_vector,
				                                         string_t(reinterpret_cast<const char *>(data), length));
			}
			FlatVector::GetData<string_t>(duckdb_vector)[row_idx] = blob_val;
			break;
		}
		case LogicalTypeId::DATE: {
			// Arrow DATE32 is days since epoch, DATE64 is milliseconds since epoch.
			date_t date_val;
			if (arrow_array->type_id() == arrow::Type::DATE32) {
				auto date_array = std::static_pointer_cast<arrow::Date32Array>(arrow_array);
				date_val = Date::EpochDaysToDate(date_array->Value(row_idx));
			} else { // DATE64
				auto date_array = std::static_pointer_cast<arrow::Date64Array>(arrow_array);
				// DATE64 is milliseconds, convert to days.
				auto ms = date_array->Value(row_idx);
				date_val = Date::EpochDaysToDate(static_cast<int32_t>(ms / (1000 * 60 * 60 * 24)));
			}
			FlatVector::GetData<date_t>(duckdb_vector)[row_idx] = date_val;
			break;
		}
		case LogicalTypeId::TIME: {
			// Arrow TIME32 (seconds or milliseconds), TIME64 (microseconds or nanoseconds).
			dtime_t time_val;
			if (arrow_array->type_id() == arrow::Type::TIME32) {
				auto time_array = std::static_pointer_cast<arrow::Time32Array>(arrow_array);
				auto time_type = std::static_pointer_cast<arrow::Time32Type>(arrow_array->type());
				int32_t value = time_array->Value(row_idx);
				if (time_type->unit() == arrow::TimeUnit::SECOND) {
					time_val = Time::FromTime(value / 3600, (value % 3600) / 60, value % 60, 0);
				} else { // MILLI
					time_val = dtime_t(static_cast<int64_t>(value) * Interval::MICROS_PER_MSEC);
				}
			} else { // TIME64
				auto time_array = std::static_pointer_cast<arrow::Time64Array>(arrow_array);
				auto time_type = std::static_pointer_cast<arrow::Time64Type>(arrow_array->type());
				int64_t value = time_array->Value(row_idx);
				if (time_type->unit() == arrow::TimeUnit::MICRO) {
					time_val = dtime_t(value);
				} else { // NANO
					time_val = dtime_t(value / 1000);
				}
			}
			FlatVector::GetData<dtime_t>(duckdb_vector)[row_idx] = time_val;
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			// Arrow TIMESTAMP can be in various units.
			auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(arrow_array);
			auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
			int64_t value = ts_array->Value(row_idx);
			timestamp_t ts_val;

			switch (ts_type->unit()) {
			case arrow::TimeUnit::SECOND:
				ts_val = Timestamp::FromEpochSeconds(value);
				break;
			case arrow::TimeUnit::MILLI:
				ts_val = Timestamp::FromEpochMs(value);
				break;
			case arrow::TimeUnit::MICRO:
				ts_val = Timestamp::FromEpochMicroSeconds(value);
				break;
			case arrow::TimeUnit::NANO:
				ts_val = Timestamp::FromEpochNanoSeconds(value);
				break;
			}
			FlatVector::GetData<timestamp_t>(duckdb_vector)[row_idx] = ts_val;
			break;
		}
		case LogicalTypeId::INTERVAL: {
			// Arrow intervals map to DuckDB intervals.
			interval_t interval_val;
			if (arrow_array->type_id() == arrow::Type::INTERVAL_MONTHS) {
				auto interval_array = std::static_pointer_cast<arrow::MonthIntervalArray>(arrow_array);
				interval_val.months = interval_array->Value(row_idx);
				interval_val.days = 0;
				interval_val.micros = 0;
			} else if (arrow_array->type_id() == arrow::Type::INTERVAL_DAY_TIME) {
				auto interval_array = std::static_pointer_cast<arrow::DayTimeIntervalArray>(arrow_array);
				auto day_time = interval_array->Value(row_idx);
				interval_val.months = 0;
				interval_val.days = day_time.days;
				interval_val.micros = static_cast<int64_t>(day_time.milliseconds) * 1000;
			} else { // INTERVAL_MONTH_DAY_NANO
				auto interval_array = std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(arrow_array);
				auto month_day_nano = interval_array->Value(row_idx);
				interval_val.months = month_day_nano.months;
				interval_val.days = month_day_nano.days;
				interval_val.micros = month_day_nano.nanoseconds / 1000;
			}
			FlatVector::GetData<interval_t>(duckdb_vector)[row_idx] = interval_val;
			break;
		}
		case LogicalTypeId::DECIMAL: {
			// Handle DECIMAL128 and DECIMAL256.
			// For now, we'll handle DECIMAL128; DECIMAL256 support would fall back to VARCHAR.
			if (arrow_array->type_id() == arrow::Type::DECIMAL128) {
				auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(arrow_array);
				auto arrow_decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(arrow_array->type());

				// Get the raw bytes from Arrow and convert to hugeint_t
				auto arrow_value = decimal_array->GetValue(row_idx);

				// Arrow stores decimal as a 128-bit integer in little-endian format
				// We need to convert it to DuckDB's internal representation
				hugeint_t value;
				// Arrow's Decimal128 provides low/high 64-bit words
				auto bytes = reinterpret_cast<const uint8_t *>(arrow_value);
				memcpy(&value.lower, bytes, sizeof(uint64_t));
				memcpy(&value.upper, bytes + sizeof(uint64_t), sizeof(int64_t));

				// DuckDB uses different physical types based on decimal width.
				auto physical_type = type.InternalType();
				switch (physical_type) {
				case PhysicalType::INT16:
					FlatVector::GetData<int16_t>(duckdb_vector)[row_idx] = static_cast<int16_t>(value.lower);
					break;
				case PhysicalType::INT32:
					FlatVector::GetData<int32_t>(duckdb_vector)[row_idx] = static_cast<int32_t>(value.lower);
					break;
				case PhysicalType::INT64:
					FlatVector::GetData<int64_t>(duckdb_vector)[row_idx] = static_cast<int64_t>(value.lower);
					break;
				case PhysicalType::INT128:
					FlatVector::GetData<hugeint_t>(duckdb_vector)[row_idx] = value;
					break;
				default:
					// Unsupported physical type for decimal.
					FlatVector::SetNull(duckdb_vector, row_idx, true);
					break;
				}
			}
			// DECIMAL256 would fall back to VARCHAR in type conversion.
			break;
		}
		default:
			// Unsupported types default to NULL.
			FlatVector::SetNull(duckdb_vector, row_idx, true);
			break;
		}
	}
}

} // namespace duckdb
