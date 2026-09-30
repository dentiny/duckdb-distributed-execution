#include "arrow_utils.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <cstring>

namespace duckdb {

namespace {

// Util function to get the index value from an Arrow dictionary indices array
int64_t GetDictionaryIndex(const std::shared_ptr<arrow::Array> &indices, idx_t arrow_idx) {
	switch (indices->type_id()) {
	case arrow::Type::INT8: {
		auto int_array = std::static_pointer_cast<arrow::Int8Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::INT16: {
		auto int_array = std::static_pointer_cast<arrow::Int16Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::INT32: {
		auto int_array = std::static_pointer_cast<arrow::Int32Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::INT64: {
		auto int_array = std::static_pointer_cast<arrow::Int64Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::UINT8: {
		auto int_array = std::static_pointer_cast<arrow::UInt8Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::UINT16: {
		auto int_array = std::static_pointer_cast<arrow::UInt16Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::UINT32: {
		auto int_array = std::static_pointer_cast<arrow::UInt32Array>(indices);
		return int_array->Value(arrow_idx);
	}
	case arrow::Type::UINT64: {
		auto int_array = std::static_pointer_cast<arrow::UInt64Array>(indices);
		return static_cast<int64_t>(int_array->Value(arrow_idx));
	}
	default:
		throw NotImplementedException("Unsupported Arrow dictionary index type: %s (type_id: %d)",
		                              indices->type()->ToString(), static_cast<int>(indices->type_id()));
	}
}

// Util function to get the string value from an Arrow dictionary at a given index
string GetDictionaryString(const std::shared_ptr<arrow::Array> &dictionary, int64_t idx) {
	if (dictionary->type_id() == arrow::Type::LARGE_STRING) {
		auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(dictionary);
		return str_array->GetString(idx);
	}

	if (dictionary->type_id() == arrow::Type::STRING) {
		auto str_array = std::static_pointer_cast<arrow::StringArray>(dictionary);
		return str_array->GetString(idx);
	}

	throw NotImplementedException(
	    "Unsupported Arrow dictionary value type: %s (type_id: %d). Expected STRING or LARGE_STRING",
	    dictionary->type()->ToString(), static_cast<int>(dictionary->type_id()));
}

// Util function to store an unsigned integer value in a DuckDB vector based on physical type.
void StoreEnumValue(Vector &duckdb_vector, idx_t duck_idx, const LogicalType &type, int64_t value) {
	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::UINT8:
		FlatVector::GetData<uint8_t>(duckdb_vector)[duck_idx] = static_cast<uint8_t>(value);
		break;
	case PhysicalType::UINT16:
		FlatVector::GetData<uint16_t>(duckdb_vector)[duck_idx] = static_cast<uint16_t>(value);
		break;
	case PhysicalType::UINT32:
		FlatVector::GetData<uint32_t>(duckdb_vector)[duck_idx] = static_cast<uint32_t>(value);
		break;
	default:
		throw NotImplementedException("Unsupported physical type %s for ENUM type %s (enum value: %lld)",
		                              TypeIdToString(physical_type), type.ToString(), value);
	}
}

// Util function to convert a single primitive element from Arrow array to DuckDB vector.
void ConvertArrowPrimitiveElement(const std::shared_ptr<arrow::Array> &arrow_array, idx_t arrow_idx,
                                  Vector &duckdb_vector, idx_t duck_idx, const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
		FlatVector::SetNull(duckdb_vector, duck_idx, true);
		break;
	case LogicalTypeId::BOOLEAN: {
		auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
		FlatVector::GetData<bool>(duckdb_vector)[duck_idx] = bool_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::TINYINT: {
		auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_array);
		FlatVector::GetData<int8_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::SMALLINT: {
		auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_array);
		FlatVector::GetData<int16_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::INTEGER: {
		auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
		FlatVector::GetData<int32_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
		FlatVector::GetData<int64_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::UTINYINT: {
		auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_array);
		FlatVector::GetData<uint8_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::USMALLINT: {
		auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_array);
		FlatVector::GetData<uint16_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::UINTEGER: {
		auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_array);
		FlatVector::GetData<uint32_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::UBIGINT: {
		auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_array);
		FlatVector::GetData<uint64_t>(duckdb_vector)[duck_idx] = int_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::FLOAT: {
		if (arrow_array->type_id() == arrow::Type::HALF_FLOAT) {
			auto half_array = std::static_pointer_cast<arrow::HalfFloatArray>(arrow_array);
			FlatVector::GetData<float>(duckdb_vector)[duck_idx] = static_cast<float>(half_array->Value(arrow_idx));
		} else {
			D_ASSERT(arrow_array->type_id() == arrow::Type::FLOAT);
			auto float_array = std::static_pointer_cast<arrow::FloatArray>(arrow_array);
			FlatVector::GetData<float>(duckdb_vector)[duck_idx] = float_array->Value(arrow_idx);
		}
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
		FlatVector::GetData<double>(duckdb_vector)[duck_idx] = double_array->Value(arrow_idx);
		break;
	}
	case LogicalTypeId::CHAR:
	case LogicalTypeId::VARCHAR: {
		if (arrow_array->type_id() == arrow::Type::DICTIONARY) {
			// Handle dictionary-encoded strings (e.g., from ENUM types)
			auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(arrow_array);
			auto idx_value = GetDictionaryIndex(dict_array->indices(), arrow_idx);
			auto str_val = GetDictionaryString(dict_array->dictionary(), idx_value);
			FlatVector::GetData<string_t>(duckdb_vector)[duck_idx] = StringVector::AddString(duckdb_vector, str_val);
		} else if (arrow_array->type_id() == arrow::Type::LARGE_STRING) {
			auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(arrow_array);
			auto str_val = str_array->GetString(arrow_idx);
			FlatVector::GetData<string_t>(duckdb_vector)[duck_idx] = StringVector::AddString(duckdb_vector, str_val);
		} else {
			D_ASSERT(arrow_array->type_id() == arrow::Type::STRING);
			auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
			auto str_val = str_array->GetString(arrow_idx);
			FlatVector::GetData<string_t>(duckdb_vector)[duck_idx] = StringVector::AddString(duckdb_vector, str_val);
		}
		break;
	}
	case LogicalTypeId::BLOB: {
		string_t blob_val;
		if (arrow_array->type_id() == arrow::Type::BINARY) {
			auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(arrow_array);
			int32_t length = 0;
			auto data = binary_array->GetValue(arrow_idx, &length);
			blob_val =
			    StringVector::AddStringOrBlob(duckdb_vector, string_t(reinterpret_cast<const char *>(data), length));
		} else if (arrow_array->type_id() == arrow::Type::LARGE_BINARY) {
			auto binary_array = std::static_pointer_cast<arrow::LargeBinaryArray>(arrow_array);
			int64_t length = 0;
			auto data = binary_array->GetValue(arrow_idx, &length);
			blob_val = StringVector::AddStringOrBlob(
			    duckdb_vector, string_t(reinterpret_cast<const char *>(data), static_cast<uint32_t>(length)));
		} else {
			D_ASSERT(arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY);
			auto binary_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_array);
			auto data = binary_array->GetValue(arrow_idx);
			auto length = binary_array->byte_width();
			blob_val =
			    StringVector::AddStringOrBlob(duckdb_vector, string_t(reinterpret_cast<const char *>(data), length));
		}
		FlatVector::GetData<string_t>(duckdb_vector)[duck_idx] = blob_val;
		break;
	}
	case LogicalTypeId::UUID: {
		hugeint_t uuid_val;
		if (arrow_array->type_id() == arrow::Type::STRING) {
			auto string_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
			auto uuid_string = string_array->GetString(arrow_idx);
			if (!UUID::FromString(uuid_string, uuid_val, true)) {
				throw ConversionException("Invalid UUID value from Arrow: %s", uuid_string);
			}
		} else if (arrow_array->type_id() == arrow::Type::LARGE_STRING) {
			auto string_array = std::static_pointer_cast<arrow::LargeStringArray>(arrow_array);
			auto uuid_string = string_array->GetString(arrow_idx);
			if (!UUID::FromString(uuid_string, uuid_val, true)) {
				throw ConversionException("Invalid UUID value from Arrow: %s", uuid_string);
			}
		} else if (arrow_array->type_id() == arrow::Type::EXTENSION) {
			auto ext_array = std::static_pointer_cast<arrow::ExtensionArray>(arrow_array);
			auto storage_array = ext_array->storage();
			if (storage_array->type_id() != arrow::Type::FIXED_SIZE_BINARY) {
				throw InternalException("Unsupported Arrow UUID storage type: %s", storage_array->type()->ToString());
			}
			auto binary_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(storage_array);
			if (binary_array->byte_width() != 16) {
				throw InternalException("Arrow UUID storage must contain 16-byte values");
			}
			uuid_val = UUID::FromBlob(binary_array->GetValue(arrow_idx));
		} else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
			auto binary_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_array);
			if (binary_array->byte_width() != 16) {
				throw InternalException("Arrow UUID storage must contain 16-byte values");
			}
			uuid_val = UUID::FromBlob(binary_array->GetValue(arrow_idx));
		} else {
			throw InternalException("Unsupported Arrow type for UUID conversion: %s", arrow_array->type()->ToString());
		}
		FlatVector::GetData<hugeint_t>(duckdb_vector)[duck_idx] = uuid_val;
		break;
	}
	case LogicalTypeId::DATE: {
		// Arrow DATE32 is days since epoch, DATE64 is milliseconds since epoch.
		date_t date_val;
		if (arrow_array->type_id() == arrow::Type::DATE32) {
			auto date_array = std::static_pointer_cast<arrow::Date32Array>(arrow_array);
			date_val = Date::EpochDaysToDate(date_array->Value(arrow_idx));
		} else {
			D_ASSERT(arrow_array->type_id() == arrow::Type::DATE64);
			auto date_array = std::static_pointer_cast<arrow::Date64Array>(arrow_array);
			auto ms = date_array->Value(arrow_idx);
			date_val = Date::EpochDaysToDate(static_cast<int32_t>(ms / (1000 * 60 * 60 * 24)));
		}
		FlatVector::GetData<date_t>(duckdb_vector)[duck_idx] = date_val;
		break;
	}
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIME_TZ: {
		if (type.id() == LogicalTypeId::TIME_NS) {
			D_ASSERT(arrow_array->type_id() == arrow::Type::TIME64);
			auto time_array = std::static_pointer_cast<arrow::Time64Array>(arrow_array);
			auto time_type = std::static_pointer_cast<arrow::Time64Type>(arrow_array->type());
			D_ASSERT(time_type->unit() == arrow::TimeUnit::NANO);
			FlatVector::GetData<dtime_ns_t>(duckdb_vector)[duck_idx] = dtime_ns_t(time_array->Value(arrow_idx));
			break;
		}

		dtime_t time_val;

		if (arrow_array->type_id() == arrow::Type::TIME32) {
			auto time_array = std::static_pointer_cast<arrow::Time32Array>(arrow_array);
			auto time_type = std::static_pointer_cast<arrow::Time32Type>(arrow_array->type());
			int32_t value = time_array->Value(arrow_idx);
			if (time_type->unit() == arrow::TimeUnit::SECOND) {
				time_val = Time::FromTime(value / 3600, (value % 3600) / 60, value % 60, 0);
			} else {
				D_ASSERT(time_type->unit() == arrow::TimeUnit::MILLI);
				time_val = dtime_t(static_cast<int64_t>(value) * Interval::MICROS_PER_MSEC);
			}
		} else {
			D_ASSERT(arrow_array->type_id() == arrow::Type::TIME64);
			auto time_array = std::static_pointer_cast<arrow::Time64Array>(arrow_array);
			auto time_type = std::static_pointer_cast<arrow::Time64Type>(arrow_array->type());
			int64_t value = time_array->Value(arrow_idx);
			if (time_type->unit() == arrow::TimeUnit::MICRO) {
				time_val = dtime_t(value);
			} else {
				D_ASSERT(time_type->unit() == arrow::TimeUnit::NANO);
				time_val = dtime_t(value / 1000);
			}
		}
		FlatVector::GetData<dtime_t>(duckdb_vector)[duck_idx] = time_val;
		break;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(arrow_array);
		auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
		int64_t value = ts_array->Value(arrow_idx);
		switch (type.id()) {
		case LogicalTypeId::TIMESTAMP_SEC:
			D_ASSERT(ts_type->unit() == arrow::TimeUnit::SECOND);
			FlatVector::GetData<timestamp_sec_t>(duckdb_vector)[duck_idx] = timestamp_sec_t(value);
			break;
		case LogicalTypeId::TIMESTAMP_MS:
			D_ASSERT(ts_type->unit() == arrow::TimeUnit::MILLI);
			FlatVector::GetData<timestamp_ms_t>(duckdb_vector)[duck_idx] = timestamp_ms_t(value);
			break;
		case LogicalTypeId::TIMESTAMP:
			D_ASSERT(ts_type->unit() == arrow::TimeUnit::MICRO);
			FlatVector::GetData<timestamp_t>(duckdb_vector)[duck_idx] = timestamp_t(value);
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			D_ASSERT(ts_type->unit() == arrow::TimeUnit::NANO);
			FlatVector::GetData<timestamp_ns_t>(duckdb_vector)[duck_idx] = timestamp_ns_t(value);
			break;
		case LogicalTypeId::TIMESTAMP_TZ:
			D_ASSERT(ts_type->unit() == arrow::TimeUnit::MICRO);
			FlatVector::GetData<timestamp_tz_t>(duckdb_vector)[duck_idx] = timestamp_tz_t(value);
			break;
		default:
			throw InternalException("Unexpected DuckDB timestamp type");
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		// Arrow intervals and durations map to DuckDB intervals.
		interval_t interval_val;
		if (arrow_array->type_id() == arrow::Type::INTERVAL_MONTHS) {
			auto interval_array = std::static_pointer_cast<arrow::MonthIntervalArray>(arrow_array);
			interval_val.months = interval_array->Value(arrow_idx);
			interval_val.days = 0;
			interval_val.micros = 0;
		} else if (arrow_array->type_id() == arrow::Type::INTERVAL_DAY_TIME) {
			auto interval_array = std::static_pointer_cast<arrow::DayTimeIntervalArray>(arrow_array);
			auto day_time = interval_array->Value(arrow_idx);
			interval_val.months = 0;
			interval_val.days = day_time.days;
			interval_val.micros = static_cast<int64_t>(day_time.milliseconds) * 1000;
		} else if (arrow_array->type_id() == arrow::Type::INTERVAL_MONTH_DAY_NANO) {
			auto interval_array = std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(arrow_array);
			auto month_day_nano = interval_array->Value(arrow_idx);
			interval_val.months = month_day_nano.months;
			interval_val.days = month_day_nano.days;
			interval_val.micros = month_day_nano.nanoseconds / 1000;
		} else if (arrow_array->type_id() == arrow::Type::DURATION) {
			auto duration_array = std::static_pointer_cast<arrow::DurationArray>(arrow_array);
			auto duration_type = std::static_pointer_cast<arrow::DurationType>(arrow_array->type());
			int64_t value = duration_array->Value(arrow_idx);

			interval_val.months = 0;
			interval_val.days = 0;

			switch (duration_type->unit()) {
			case arrow::TimeUnit::SECOND:
				interval_val.micros = value * 1000000;
				break;
			case arrow::TimeUnit::MILLI:
				interval_val.micros = value * 1000;
				break;
			case arrow::TimeUnit::MICRO:
				interval_val.micros = value;
				break;
			case arrow::TimeUnit::NANO:
				interval_val.micros = value / 1000;
				break;
			}
		}
		FlatVector::GetData<interval_t>(duckdb_vector)[duck_idx] = interval_val;
		break;
	}
	case LogicalTypeId::HUGEINT: {
		if (arrow_array->type_id() != arrow::Type::DECIMAL128) {
			throw InternalException("Unsupported Arrow type for HUGEINT conversion: %s",
			                        arrow_array->type()->ToString());
		}
		auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(arrow_array);
		hugeint_t value;
		memcpy(&value, decimal_array->GetValue(arrow_idx), sizeof(value));
		FlatVector::GetData<hugeint_t>(duckdb_vector)[duck_idx] = value;
		break;
	}
	case LogicalTypeId::UHUGEINT: {
		if (arrow_array->type_id() != arrow::Type::DECIMAL128) {
			throw InternalException("Unsupported Arrow type for UHUGEINT conversion: %s",
			                        arrow_array->type()->ToString());
		}
		auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(arrow_array);
		uhugeint_t value;
		memcpy(&value, decimal_array->GetValue(arrow_idx), sizeof(value));
		FlatVector::GetData<uhugeint_t>(duckdb_vector)[duck_idx] = value;
		break;
	}
	case LogicalTypeId::DECIMAL: {
		if (arrow_array->type_id() == arrow::Type::DECIMAL128) {
			auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(arrow_array);
			auto arrow_value = decimal_array->GetValue(arrow_idx);

			hugeint_t value;
			auto bytes = reinterpret_cast<const uint8_t *>(arrow_value);
			memcpy(&value.lower, bytes, sizeof(uint64_t));
			memcpy(&value.upper, bytes + sizeof(uint64_t), sizeof(int64_t));

			// DuckDB uses different physical types based on decimal width.
			auto physical_type = type.InternalType();
			switch (physical_type) {
			case PhysicalType::INT16:
				FlatVector::GetData<int16_t>(duckdb_vector)[duck_idx] = static_cast<int16_t>(value.lower);
				break;
			case PhysicalType::INT32:
				FlatVector::GetData<int32_t>(duckdb_vector)[duck_idx] = static_cast<int32_t>(value.lower);
				break;
			case PhysicalType::INT64:
				FlatVector::GetData<int64_t>(duckdb_vector)[duck_idx] = static_cast<int64_t>(value.lower);
				break;
			case PhysicalType::INT128:
				FlatVector::GetData<hugeint_t>(duckdb_vector)[duck_idx] = value;
				break;
			default:
				throw NotImplementedException("Unsupported physical type %s for DECIMAL %s",
				                              TypeIdToString(physical_type), type.ToString());
			}
		} else {
			throw NotImplementedException("Unsupported Arrow decimal type %s", arrow_array->type()->ToString());
		}
		break;
	}
	case LogicalTypeId::ENUM: {
		// Handle Arrow dictionary-encoded arrays for ENUM types.
		if (arrow_array->type_id() == arrow::Type::DICTIONARY) {
			auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(arrow_array);

			// Get the string value from the Arrow dictionary
			auto arrow_idx_value = GetDictionaryIndex(dict_array->indices(), arrow_idx);
			auto str_val = GetDictionaryString(dict_array->dictionary(), arrow_idx_value);

			// Convert the string to a DuckDB ENUM index.
			string_t enum_str {str_val};
			int64_t enum_idx = EnumType::GetPos(type, enum_str);

			if (enum_idx < 0) {
				throw InvalidInputException("ENUM value '%s' not found in type %s", str_val, type.ToString());
			}

			// Store the index in the DuckDB vector based on its physical type.
			StoreEnumValue(duckdb_vector, duck_idx, type, enum_idx);
		} else if (arrow_array->type_id() == arrow::Type::STRING ||
		           arrow_array->type_id() == arrow::Type::LARGE_STRING) {
			// Handle case where ENUM is sent as plain STRING/LARGE_STRING
			// This can happen when the server sends ENUM values as their string representation
			string str_val;
			if (arrow_array->type_id() == arrow::Type::LARGE_STRING) {
				auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(arrow_array);
				str_val = str_array->GetString(arrow_idx);
			} else {
				auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
				str_val = str_array->GetString(arrow_idx);
			}

			// Convert the string to a DuckDB ENUM index.
			string_t enum_str {str_val};
			int64_t enum_idx = EnumType::GetPos(type, enum_str);

			if (enum_idx < 0) {
				throw InvalidInputException("ENUM value '%s' not found in type %s", str_val, type.ToString());
			}

			// Store the index in the DuckDB vector based on its physical type.
			StoreEnumValue(duckdb_vector, duck_idx, type, enum_idx);
		} else {
			// Handle case where ENUM is sent as its physical type (UINT8/UINT16/UINT32).
			int64_t enum_idx = 0;
			switch (arrow_array->type_id()) {
			case arrow::Type::UINT8: {
				auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			case arrow::Type::UINT16: {
				auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			case arrow::Type::UINT32: {
				auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			case arrow::Type::UINT64: {
				auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_array);
				enum_idx = static_cast<int64_t>(int_array->Value(arrow_idx));
				break;
			}
			case arrow::Type::INT8: {
				auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			case arrow::Type::INT16: {
				auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			case arrow::Type::INT32: {
				auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			case arrow::Type::INT64: {
				auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
				enum_idx = int_array->Value(arrow_idx);
				break;
			}
			default: {
				auto arrow_type_name = arrow_array->type()->ToString();
				throw NotImplementedException("ENUM type received unexpected Arrow type: %s (type_id: %d). "
				                              "Expected DICTIONARY or integer types (INT8/16/32/64 or UINT8/16/32/64). "
				                              "DuckDB ENUM type: %s",
				                              arrow_type_name, static_cast<int>(arrow_array->type_id()),
				                              type.ToString());
			}
			}

			StoreEnumValue(duckdb_vector, duck_idx, type, enum_idx);
		}
		break;
	}
	default:
		throw NotImplementedException("Arrow to DuckDB conversion not implemented for type: %s", type.ToString());
	}
}

// Util function to append a list entry of the given length to a DuckDB LIST or MAP vector, returns the child offset.
idx_t AppendListEntry(Vector &list_vector, idx_t duck_idx, idx_t length) {
	auto old_size = ListVector::GetListSize(list_vector);
	auto &entry = FlatVector::GetData<list_entry_t>(list_vector)[duck_idx];
	entry.offset = old_size;
	entry.length = length;
	ListVector::Reserve(list_vector, old_size + length);
	ListVector::SetListSize(list_vector, old_size + length);
	return old_size;
}

void ConvertArrowElement(const std::shared_ptr<arrow::Array> &arrow_array, idx_t arrow_idx, Vector &duckdb_vector,
                         idx_t duck_idx, const LogicalType &type) {
	if (arrow_array->IsNull(arrow_idx)) {
		FlatVector::SetNull(duckdb_vector, duck_idx, true);
		return;
	}

	if (type.id() == LogicalTypeId::MAP) {
		if (arrow_array->type_id() != arrow::Type::MAP) {
			throw InternalException("Expected Arrow MAP for DuckDB type %s, received %s", type.ToString(),
			                        arrow_array->type()->ToString());
		}
		auto map_array = std::static_pointer_cast<arrow::MapArray>(arrow_array);
		auto offset = NumericCast<idx_t>(map_array->value_offset(arrow_idx));
		auto length = NumericCast<idx_t>(map_array->value_length(arrow_idx));
		auto child_offset = AppendListEntry(duckdb_vector, duck_idx, length);

		auto &key_vector = MapVector::GetKeys(duckdb_vector);
		auto &value_vector = MapVector::GetValues(duckdb_vector);
		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		for (idx_t child_idx = 0; child_idx < length; child_idx++) {
			ConvertArrowElement(map_array->keys(), offset + child_idx, key_vector, child_offset + child_idx, key_type);
			ConvertArrowElement(map_array->items(), offset + child_idx, value_vector, child_offset + child_idx,
			                    value_type);
		}
		return;
	}

	if (type.id() == LogicalTypeId::LIST) {
		std::shared_ptr<arrow::Array> child_array;
		int64_t offset = 0;
		int64_t length = 0;
		if (arrow_array->type_id() == arrow::Type::LIST) {
			auto list_array = std::static_pointer_cast<arrow::ListArray>(arrow_array);
			child_array = list_array->values();
			offset = list_array->value_offset(arrow_idx);
			length = list_array->value_length(arrow_idx);
		} else if (arrow_array->type_id() == arrow::Type::LARGE_LIST) {
			auto list_array = std::static_pointer_cast<arrow::LargeListArray>(arrow_array);
			child_array = list_array->values();
			offset = list_array->value_offset(arrow_idx);
			length = list_array->value_length(arrow_idx);
		} else {
			throw InternalException("Expected Arrow LIST for DuckDB type %s, received %s", type.ToString(),
			                        arrow_array->type()->ToString());
		}

		auto child_length = NumericCast<idx_t>(length);
		auto child_offset = AppendListEntry(duckdb_vector, duck_idx, child_length);

		auto &child_vector = ListVector::GetEntry(duckdb_vector);
		auto &child_type = ListType::GetChildType(type);
		for (idx_t child_idx = 0; child_idx < child_length; child_idx++) {
			ConvertArrowElement(child_array, NumericCast<idx_t>(offset) + child_idx, child_vector,
			                    child_offset + child_idx, child_type);
		}
		return;
	}

	if (type.id() == LogicalTypeId::ARRAY) {
		if (arrow_array->type_id() != arrow::Type::FIXED_SIZE_LIST) {
			throw InternalException("Expected Arrow FIXED_SIZE_LIST for DuckDB type %s, received %s", type.ToString(),
			                        arrow_array->type()->ToString());
		}
		auto array = std::static_pointer_cast<arrow::FixedSizeListArray>(arrow_array);
		auto array_size = ArrayType::GetSize(type);
		if (NumericCast<idx_t>(array->value_length(arrow_idx)) != array_size) {
			throw InternalException("Arrow fixed-size list length does not match DuckDB type %s", type.ToString());
		}

		auto &child_vector = ArrayVector::GetEntry(duckdb_vector);
		auto &child_type = ArrayType::GetChildType(type);
		auto child_offset = NumericCast<idx_t>(array->value_offset(arrow_idx));
		auto target_offset = duck_idx * array_size;
		for (idx_t child_idx = 0; child_idx < array_size; child_idx++) {
			ConvertArrowElement(array->values(), child_offset + child_idx, child_vector, target_offset + child_idx,
			                    child_type);
		}
		return;
	}

	if (type.id() == LogicalTypeId::STRUCT) {
		if (arrow_array->type_id() != arrow::Type::STRUCT) {
			throw InternalException("Expected Arrow STRUCT for DuckDB type %s, received %s", type.ToString(),
			                        arrow_array->type()->ToString());
		}
		auto struct_array = std::static_pointer_cast<arrow::StructArray>(arrow_array);
		auto &child_types = StructType::GetChildTypes(type);
		if (NumericCast<idx_t>(struct_array->num_fields()) != child_types.size()) {
			throw InternalException("Arrow struct field count %d does not match DuckDB type %s",
			                        struct_array->num_fields(), type.ToString());
		}

		auto &child_vectors = StructVector::GetEntries(duckdb_vector);
		for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
			// StructArray::field() applies the parent array offset to the child array.
			ConvertArrowElement(struct_array->field(NumericCast<int>(child_idx)), arrow_idx, *child_vectors[child_idx],
			                    duck_idx, child_types[child_idx].second);
		}
		return;
	}

	ConvertArrowPrimitiveElement(arrow_array, arrow_idx, duckdb_vector, duck_idx, type);
}

} // namespace

LogicalType ArrowTypeToDuckDBType(const std::shared_ptr<arrow::DataType> &arrow_type) {
	// TODO(hjiang):
	// 1. Add support for complex nested types (UNION).
	// 2. Add support for special types (ENUM, BIT, BIGNUM).
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
	// Half-precision floats convert to regular float.
	case arrow::Type::HALF_FLOAT:
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
	case arrow::Type::EXTENSION: {
		auto ext_type = std::static_pointer_cast<arrow::ExtensionType>(arrow_type);
		auto ext_name = ext_type->extension_name();

		// Check for known canonical extension types.
		if (ext_name == "arrow.uuid") {
			return LogicalType {LogicalTypeId::UUID};
		}

		// Fallbacks to storage type for other extensions types.
		return ArrowTypeToDuckDBType(ext_type->storage_type());
	}
	case arrow::Type::DATE32:
	case arrow::Type::DATE64:
		return LogicalType {LogicalTypeId::DATE};
	case arrow::Type::TIME32:
	case arrow::Type::TIME64: {
		// Note: Arrow doesn't have a native TIME_TZ type, but we handle nanosecond precision.
		auto time_type = std::static_pointer_cast<arrow::TimeType>(arrow_type);
		if (time_type->unit() == arrow::TimeUnit::NANO) {
			return LogicalType {LogicalTypeId::TIME_NS};
		}
		return LogicalType {LogicalTypeId::TIME};
	}
	case arrow::Type::TIMESTAMP: {
		auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_type);

		// If timezone is present, use TIMESTAMP_TZ.
		if (!ts_type->timezone().empty()) {
			return LogicalType {LogicalTypeId::TIMESTAMP_TZ};
		}

		// Otherwise map based on time unit.
		switch (ts_type->unit()) {
		case arrow::TimeUnit::SECOND:
			return LogicalType {LogicalTypeId::TIMESTAMP_SEC};
		case arrow::TimeUnit::MILLI:
			return LogicalType {LogicalTypeId::TIMESTAMP_MS};
		case arrow::TimeUnit::MICRO:
			// Duckdb timestamp defaults microseconds.
			return LogicalType {LogicalTypeId::TIMESTAMP};
		case arrow::TimeUnit::NANO:
			return LogicalType {LogicalTypeId::TIMESTAMP_NS};
		default:
			return LogicalType {LogicalTypeId::TIMESTAMP};
		}
	}
	case arrow::Type::INTERVAL_MONTHS:
	case arrow::Type::INTERVAL_DAY_TIME:
	case arrow::Type::INTERVAL_MONTH_DAY_NANO:
		return LogicalType {LogicalTypeId::INTERVAL};
	case arrow::Type::DURATION:
		return LogicalType {LogicalTypeId::INTERVAL};
	case arrow::Type::DECIMAL128:
	case arrow::Type::DECIMAL256: {
		auto decimal_type = std::static_pointer_cast<arrow::DecimalType>(arrow_type);
		return LogicalType::DECIMAL(decimal_type->precision(), decimal_type->scale());
	}
	case arrow::Type::LIST:
	case arrow::Type::LARGE_LIST: {
		auto list_type = std::static_pointer_cast<arrow::BaseListType>(arrow_type);
		auto child_type = ArrowTypeToDuckDBType(list_type->value_type());
		return LogicalType::LIST(child_type);
	}
	case arrow::Type::FIXED_SIZE_LIST: {
		auto array_type = std::static_pointer_cast<arrow::FixedSizeListType>(arrow_type);
		auto child_type = ArrowTypeToDuckDBType(array_type->value_type());
		return LogicalType::ARRAY(child_type, NumericCast<idx_t>(array_type->list_size()));
	}
	case arrow::Type::MAP: {
		auto map_type = std::static_pointer_cast<arrow::MapType>(arrow_type);
		auto key_type = ArrowTypeToDuckDBType(map_type->key_type());
		auto value_type = ArrowTypeToDuckDBType(map_type->item_type());
		return LogicalType::MAP(std::move(key_type), std::move(value_type));
	}
	case arrow::Type::STRUCT: {
		child_list_t<LogicalType> child_types;
		for (const auto &field : arrow_type->fields()) {
			child_types.emplace_back(field->name(), ArrowTypeToDuckDBType(field->type()));
		}
		return LogicalType::STRUCT(std::move(child_types));
	}
	case arrow::Type::DICTIONARY: {
		// Arrow dictionary types map to DuckDB ENUM types.
		// The dictionary contains the enum values (strings), and indices reference them.
		auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(arrow_type);
		auto value_type = dict_type->value_type();

		// We only support string dictionaries for enum conversion.
		if (value_type->id() != arrow::Type::STRING && value_type->id() != arrow::Type::LARGE_STRING) {
			// Fallback to the index type for non-string dictionaries.
			return ArrowTypeToDuckDBType(dict_type->index_type());
		}

		// For dictionary-encoded strings, we return VARCHAR since we need the actual dictionary values to create a
		// proper ENUM type. The ENUM creation will be handled during table creation or when the full schema is known.
		return LogicalType {LogicalTypeId::VARCHAR};
	}
	default:
		// Fallback to VARCHAR for unsupported types (UNION, etc.).
		return LogicalType {LogicalTypeId::VARCHAR};
	}
}

void ConvertArrowArrayToDuckDBVector(const std::shared_ptr<arrow::Array> &arrow_array, Vector &duckdb_vector,
                                     const LogicalType &type, idx_t num_rows) {
	for (idx_t row_idx = 0; row_idx < num_rows; ++row_idx) {
		ConvertArrowElement(arrow_array, row_idx, duckdb_vector, row_idx, type);
	}
}

unique_ptr<QueryResult> MakeArrowResult(StatementType statement_type,
                                        const vector<std::shared_ptr<arrow::RecordBatch>> &batches,
                                        const std::shared_ptr<arrow::Schema> &schema,
                                        const vector<LogicalType> *expected_types) {
	vector<string> names;
	vector<LogicalType> types;
	if (expected_types) {
		types = *expected_types;
	}
	if (schema) {
		for (const auto &field : schema->fields()) {
			names.emplace_back(field->name());
			if (!expected_types) {
				types.emplace_back(ArrowTypeToDuckDBType(field->type()));
			}
		}
	} else if (expected_types) {
		names.resize(types.size());
	}

	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	for (const auto &batch : batches) {
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), types);
		for (int col_idx = 0; col_idx < batch->num_columns(); ++col_idx) {
			ConvertArrowArrayToDuckDBVector(batch->column(col_idx), chunk.data[col_idx], types[col_idx],
			                                batch->num_rows());
		}
		chunk.SetCardinality(batch->num_rows());
		collection->Append(chunk);
	}
	return make_uniq<MaterializedQueryResult>(statement_type, StatementProperties(), names, std::move(collection),
	                                          ClientProperties());
}

} // namespace duckdb
