#include "catch/catch.hpp"

#include "arrow_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector.hpp"

#include <array>
#include <arrow/builder.h>

using namespace duckdb;

TEST_CASE("Arrow UUID conversion", "[arrow][uuid]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto &context = *connection.context;
	const auto uuid_type = LogicalType::UUID;

	SECTION("UTF-8 strings") {
		arrow::StringBuilder builder;
		REQUIRE(builder.Append("550e8400-e29b-41d4-a716-446655440000").ok());
		REQUIRE(builder.Append("6ba7b810-9dad-11d1-80b4-00c04fd430c8").ok());
		REQUIRE(builder.AppendNull().ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector result(uuid_type, 3);
		ConvertArrowArrayToDuckDBVector(context, arrow_array, result, uuid_type, 3);

		auto values = FlatVector::GetData<hugeint_t>(result);
		REQUIRE(UUID::ToString(values[0]) == "550e8400-e29b-41d4-a716-446655440000");
		REQUIRE(UUID::ToString(values[1]) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
		REQUIRE(!FlatVector::Validity(result).RowIsValid(2));
	}

	SECTION("Canonical 16-byte binary") {
		const std::array<uint8_t, 16> uuid_bytes {
		    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		};
		arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(uuid_bytes.size()));
		REQUIRE(builder.Append(uuid_bytes.data()).ok());

		std::shared_ptr<arrow::Array> arrow_array;
		REQUIRE(builder.Finish(&arrow_array).ok());

		Vector result(uuid_type, 1);
		ConvertArrowArrayToDuckDBVector(context, arrow_array, result, uuid_type, 1);

		auto value = FlatVector::GetData<hugeint_t>(result)[0];
		REQUIRE(UUID::ToString(value) == "550e8400-e29b-41d4-a716-446655440000");
	}
}
