#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "arrow_utils.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>

using namespace duckdb;

TEST_CASE("Arrow ENUM Conversion Tests", "[arrow][enum]") {
	// Create a DuckDB ENUM type: ENUM('happy', 'sad', 'neutral')
	Vector enum_values(LogicalType::VARCHAR, 3);
	auto values_ptr = FlatVector::GetData<string_t>(enum_values);
	values_ptr[0] = StringVector::AddString(enum_values, "happy");
	values_ptr[1] = StringVector::AddString(enum_values, "sad");
	values_ptr[2] = StringVector::AddString(enum_values, "neutral");

	auto enum_type = LogicalType::ENUM("mood", enum_values, 3);

	SECTION("Convert from Arrow STRING") {
		// This is the case that was failing in CI
		arrow::StringBuilder builder;
		REQUIRE(builder.Append("happy").ok());
		REQUIRE(builder.Append("sad").ok());
		REQUIRE(builder.Append("neutral").ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		// Convert to DuckDB vector
		Vector duckdb_vector(enum_type, 3);
		ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 3);

		// Verify the conversion
		auto data_ptr = FlatVector::GetData<uint8_t>(duckdb_vector);
		REQUIRE(data_ptr[0] == 0); // 'happy'
		REQUIRE(data_ptr[1] == 1); // 'sad'
		REQUIRE(data_ptr[2] == 2); // 'neutral'
	}

	SECTION("Convert from Arrow LARGE_STRING") {
		arrow::LargeStringBuilder builder;
		REQUIRE(builder.Append("sad").ok());
		REQUIRE(builder.Append("neutral").ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector duckdb_vector(enum_type, 2);
		ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 2);

		auto data_ptr = FlatVector::GetData<uint8_t>(duckdb_vector);
		REQUIRE(data_ptr[0] == 1); // 'sad'
		REQUIRE(data_ptr[1] == 2); // 'neutral'
	}

	SECTION("Convert from Arrow DICTIONARY") {
		// Create dictionary array
		arrow::StringBuilder dict_builder;
		REQUIRE(dict_builder.Append("happy").ok());
		REQUIRE(dict_builder.Append("sad").ok());
		REQUIRE(dict_builder.Append("neutral").ok());

		std::shared_ptr<arrow::Array> dict_array;
		REQUIRE(dict_builder.Finish(&dict_array).ok());

		// Create indices array
		arrow::UInt8Builder indices_builder;
		REQUIRE(indices_builder.Append(0).ok()); // happy
		REQUIRE(indices_builder.Append(2).ok()); // neutral
		REQUIRE(indices_builder.Append(1).ok()); // sad

		std::shared_ptr<arrow::Array> indices_array;
		REQUIRE(indices_builder.Finish(&indices_array).ok());

		// Create dictionary array
		auto dict_type = std::make_shared<arrow::DictionaryType>(arrow::uint8(), arrow::utf8());
		auto arrow_array = std::make_shared<arrow::DictionaryArray>(dict_type, indices_array, dict_array);

		Vector duckdb_vector(enum_type, 3);
		ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 3);

		auto data_ptr = FlatVector::GetData<uint8_t>(duckdb_vector);
		REQUIRE(data_ptr[0] == 0); // 'happy'
		REQUIRE(data_ptr[1] == 2); // 'neutral'
		REQUIRE(data_ptr[2] == 1); // 'sad'
	}

	SECTION("Convert from Arrow UINT8 (physical type)") {
		arrow::UInt8Builder builder;
		REQUIRE(builder.Append(0).ok()); // happy
		REQUIRE(builder.Append(1).ok()); // sad
		REQUIRE(builder.Append(2).ok()); // neutral

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector duckdb_vector(enum_type, 3);
		ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 3);

		auto data_ptr = FlatVector::GetData<uint8_t>(duckdb_vector);
		REQUIRE(data_ptr[0] == 0);
		REQUIRE(data_ptr[1] == 1);
		REQUIRE(data_ptr[2] == 2);
	}

	SECTION("Convert with NULL values") {
		arrow::StringBuilder builder;
		REQUIRE(builder.Append("happy").ok());
		REQUIRE(builder.AppendNull().ok());
		REQUIRE(builder.Append("neutral").ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector duckdb_vector(enum_type, 3);
		ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 3);

		auto data_ptr = FlatVector::GetData<uint8_t>(duckdb_vector);
		auto &validity = FlatVector::Validity(duckdb_vector);

		REQUIRE(data_ptr[0] == 0);        // 'happy'
		REQUIRE(!validity.RowIsValid(1)); // NULL
		REQUIRE(data_ptr[2] == 2);        // 'neutral'
	}

	SECTION("Invalid enum value throws error") {
		arrow::StringBuilder builder;
		REQUIRE(builder.Append("happy").ok());
		REQUIRE(builder.Append("invalid_value").ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector duckdb_vector(enum_type, 2);

		// Should throw InvalidInputException
		REQUIRE_THROWS_AS(ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 2),
		                  InvalidInputException);
	}
}

TEST_CASE("Arrow ENUM with larger physical types", "[arrow][enum]") {
	// Create ENUM with more than 256 values (requires UINT16)
	Vector enum_values(LogicalType::VARCHAR, 300);
	for (idx_t idx = 0; idx < 300; idx++) {
		auto str = "value_" + std::to_string(idx);
		FlatVector::GetData<string_t>(enum_values)[idx] = StringVector::AddString(enum_values, str);
	}

	auto enum_type = LogicalType::ENUM("large_enum", enum_values, 300);

	SECTION("Convert from UINT16") {
		arrow::UInt16Builder builder;
		REQUIRE(builder.Append(0).ok());
		REQUIRE(builder.Append(100).ok());
		REQUIRE(builder.Append(299).ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector duckdb_vector(enum_type, 3);
		ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, enum_type, 3);

		auto data_ptr = FlatVector::GetData<uint16_t>(duckdb_vector);
		REQUIRE(data_ptr[0] == 0);
		REQUIRE(data_ptr[1] == 100);
		REQUIRE(data_ptr[2] == 299);
	}
}
