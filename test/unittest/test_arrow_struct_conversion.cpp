#include "catch/catch.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/types/vector.hpp"

#include <arrow/builder.h>

using namespace duckdb;

TEST_CASE("Arrow STRUCT conversion", "[arrow][struct]") {
	auto id_builder = std::make_shared<arrow::Int32Builder>();
	auto name_builder = std::make_shared<arrow::StringBuilder>();
	auto arrow_type = arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
	arrow::StructBuilder struct_builder(arrow_type, arrow::default_memory_pool(), {id_builder, name_builder});

	REQUIRE(struct_builder.Append().ok());
	REQUIRE(id_builder->Append(1).ok());
	REQUIRE(name_builder->Append("alice").ok());
	REQUIRE(struct_builder.AppendNull().ok());
	REQUIRE(struct_builder.Append().ok());
	REQUIRE(id_builder->Append(3).ok());
	REQUIRE(name_builder->AppendNull().ok());

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(struct_builder.Finish(&arrow_array).ok());

	child_list_t<LogicalType> child_types;
	child_types.emplace_back("id", LogicalType::INTEGER);
	child_types.emplace_back("name", LogicalType::VARCHAR);
	auto type = LogicalType::STRUCT(std::move(child_types));
	REQUIRE(ArrowTypeToDuckDBType(arrow_array->type()) == type);

	Vector result(type, 3);
	ConvertArrowArrayToDuckDBVector(arrow_array, result, type, 3);

	REQUIRE(Value::NotDistinctFrom(result.GetValue(0),
	                               Value::STRUCT({{"id", Value::INTEGER(1)}, {"name", Value("alice")}})));
	REQUIRE(result.GetValue(1).IsNull());
	REQUIRE(Value::NotDistinctFrom(result.GetValue(2),
	                               Value::STRUCT({{"id", Value::INTEGER(3)}, {"name", Value(LogicalType::VARCHAR)}})));

	auto &entries = StructVector::GetEntries(result);
	REQUIRE(!FlatVector::Validity(*entries[0]).RowIsValid(1));
	REQUIRE(!FlatVector::Validity(*entries[1]).RowIsValid(1));
}

TEST_CASE("Arrow nested STRUCT conversion", "[arrow][struct]") {
	auto x_builder = std::make_shared<arrow::DoubleBuilder>();
	auto inner_type = arrow::struct_({arrow::field("x", arrow::float64())});
	auto inner_builder = std::make_shared<arrow::StructBuilder>(
	    inner_type, arrow::default_memory_pool(), std::vector<std::shared_ptr<arrow::ArrayBuilder>> {x_builder});
	auto tag_values_builder = std::make_shared<arrow::StringBuilder>();
	auto tags_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), tag_values_builder);
	auto outer_type = arrow::struct_({arrow::field("point", inner_type), arrow::field("tags", tags_builder->type())});
	arrow::StructBuilder outer_builder(outer_type, arrow::default_memory_pool(), {inner_builder, tags_builder});

	REQUIRE(outer_builder.Append().ok());
	REQUIRE(inner_builder->Append().ok());
	REQUIRE(x_builder->Append(1.5).ok());
	REQUIRE(tags_builder->Append().ok());
	REQUIRE(tag_values_builder->Append("a").ok());
	REQUIRE(tag_values_builder->Append("b").ok());

	REQUIRE(outer_builder.Append().ok());
	REQUIRE(inner_builder->AppendNull().ok());
	REQUIRE(tags_builder->Append().ok());
	REQUIRE(tag_values_builder->Append("c").ok());

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(outer_builder.Finish(&arrow_array).ok());

	child_list_t<LogicalType> inner_children;
	inner_children.emplace_back("x", LogicalType::DOUBLE);
	child_list_t<LogicalType> outer_children;
	outer_children.emplace_back("point", LogicalType::STRUCT(inner_children));
	outer_children.emplace_back("tags", LogicalType::LIST(LogicalType::VARCHAR));
	auto type = LogicalType::STRUCT(std::move(outer_children));
	REQUIRE(ArrowTypeToDuckDBType(arrow_array->type()) == type);

	Vector result(type, 2);
	ConvertArrowArrayToDuckDBVector(arrow_array, result, type, 2);

	auto expected_row0 = Value::STRUCT({{"point", Value::STRUCT({{"x", Value::DOUBLE(1.5)}})},
	                                    {"tags", Value::LIST(LogicalType::VARCHAR, {Value("a"), Value("b")})}});
	auto expected_row1 = Value::STRUCT({{"point", Value(LogicalType::STRUCT(inner_children))},
	                                    {"tags", Value::LIST(LogicalType::VARCHAR, {Value("c")})}});
	REQUIRE(Value::NotDistinctFrom(result.GetValue(0), expected_row0));
	REQUIRE(Value::NotDistinctFrom(result.GetValue(1), expected_row1));
}

TEST_CASE("Arrow sliced STRUCT conversion", "[arrow][struct]") {
	auto id_builder = std::make_shared<arrow::Int32Builder>();
	auto arrow_type = arrow::struct_({arrow::field("id", arrow::int32())});
	arrow::StructBuilder struct_builder(arrow_type, arrow::default_memory_pool(), {id_builder});
	for (int32_t value = 0; value < 4; value++) {
		REQUIRE(struct_builder.Append().ok());
		REQUIRE(id_builder->Append(value * 10).ok());
	}

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(struct_builder.Finish(&arrow_array).ok());
	auto sliced_array = arrow_array->Slice(2, 2);

	auto type = ArrowTypeToDuckDBType(sliced_array->type());
	Vector result(type, 2);
	ConvertArrowArrayToDuckDBVector(sliced_array, result, type, 2);

	REQUIRE(Value::NotDistinctFrom(result.GetValue(0), Value::STRUCT({{"id", Value::INTEGER(20)}})));
	REQUIRE(Value::NotDistinctFrom(result.GetValue(1), Value::STRUCT({{"id", Value::INTEGER(30)}})));
}
