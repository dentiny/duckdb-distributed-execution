#include "catch/catch.hpp"

#include "arrow_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/vector.hpp"

#include <arrow/builder.h>

using namespace duckdb;

TEST_CASE("Arrow MAP conversion", "[arrow][map]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto &context = *connection.context;

	auto key_builder = std::make_shared<arrow::StringBuilder>();
	auto item_builder = std::make_shared<arrow::Int32Builder>();
	arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, item_builder);

	REQUIRE(map_builder.Append().ok());
	REQUIRE(key_builder->Append("a").ok());
	REQUIRE(item_builder->Append(1).ok());
	REQUIRE(key_builder->Append("b").ok());
	REQUIRE(item_builder->AppendNull().ok());
	REQUIRE(map_builder.AppendNull().ok());
	REQUIRE(map_builder.Append().ok());
	REQUIRE(map_builder.Append().ok());
	REQUIRE(key_builder->Append("c").ok());
	REQUIRE(item_builder->Append(3).ok());

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(map_builder.Finish(&arrow_array).ok());

	auto type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
	REQUIRE(ArrowTypeToDuckDBType(context, arrow_array->type()) == type);

	Vector result(type, 4);
	ConvertArrowArrayToDuckDBVector(context, arrow_array, result, type, 4);

	REQUIRE(result.GetValue(0) == Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("a"), Value("b")},
	                                         {Value::INTEGER(1), Value(LogicalType::INTEGER)}));
	REQUIRE(result.GetValue(1).IsNull());
	REQUIRE(result.GetValue(2) == Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {}, {}));
	REQUIRE(result.GetValue(3) ==
	        Value::MAP(LogicalType::VARCHAR, LogicalType::INTEGER, {Value("c")}, {Value::INTEGER(3)}));
}

TEST_CASE("Arrow MAP with nested LIST values conversion", "[arrow][map]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto &context = *connection.context;

	auto key_builder = std::make_shared<arrow::Int64Builder>();
	auto values_builder = std::make_shared<arrow::DoubleBuilder>();
	auto item_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), values_builder);
	arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, item_builder);

	REQUIRE(map_builder.Append().ok());
	REQUIRE(key_builder->Append(10).ok());
	REQUIRE(item_builder->Append().ok());
	REQUIRE(values_builder->Append(1.5).ok());
	REQUIRE(values_builder->Append(2.5).ok());
	REQUIRE(key_builder->Append(20).ok());
	REQUIRE(item_builder->AppendNull().ok());

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(map_builder.Finish(&arrow_array).ok());

	auto value_type = LogicalType::LIST(LogicalType::DOUBLE);
	auto type = LogicalType::MAP(LogicalType::BIGINT, value_type);
	REQUIRE(ArrowTypeToDuckDBType(context, arrow_array->type()) == type);

	Vector result(type, 1);
	ConvertArrowArrayToDuckDBVector(context, arrow_array, result, type, 1);

	auto expected =
	    Value::MAP(LogicalType::BIGINT, value_type, {Value::BIGINT(10), Value::BIGINT(20)},
	               {Value::LIST(LogicalType::DOUBLE, {Value::DOUBLE(1.5), Value::DOUBLE(2.5)}), Value(value_type)});
	REQUIRE(result.GetValue(0) == expected);
}

TEST_CASE("Arrow sliced MAP conversion", "[arrow][map]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto &context = *connection.context;

	auto key_builder = std::make_shared<arrow::Int32Builder>();
	auto item_builder = std::make_shared<arrow::Int32Builder>();
	arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, item_builder);
	for (int32_t value = 0; value < 4; value++) {
		REQUIRE(map_builder.Append().ok());
		REQUIRE(key_builder->Append(value).ok());
		REQUIRE(item_builder->Append(value * 10).ok());
	}

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(map_builder.Finish(&arrow_array).ok());
	auto sliced_array = arrow_array->Slice(2, 2);

	auto type = ArrowTypeToDuckDBType(context, sliced_array->type());
	Vector result(type, 2);
	ConvertArrowArrayToDuckDBVector(context, sliced_array, result, type, 2);

	REQUIRE(result.GetValue(0) ==
	        Value::MAP(LogicalType::INTEGER, LogicalType::INTEGER, {Value::INTEGER(2)}, {Value::INTEGER(20)}));
	REQUIRE(result.GetValue(1) ==
	        Value::MAP(LogicalType::INTEGER, LogicalType::INTEGER, {Value::INTEGER(3)}, {Value::INTEGER(30)}));
}
