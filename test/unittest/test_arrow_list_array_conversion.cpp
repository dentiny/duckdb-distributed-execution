#include "catch/catch.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/types/vector.hpp"

#include <arrow/builder.h>

using namespace duckdb;

TEST_CASE("Arrow nested LIST conversion", "[arrow][list]") {
	auto values_builder = std::make_shared<arrow::Int32Builder>();
	auto inner_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), values_builder);
	arrow::ListBuilder outer_builder(arrow::default_memory_pool(), inner_builder);

	REQUIRE(outer_builder.Append().ok());
	REQUIRE(inner_builder->Append().ok());
	REQUIRE(values_builder->Append(1).ok());
	REQUIRE(values_builder->Append(2).ok());
	REQUIRE(inner_builder->Append().ok());
	REQUIRE(values_builder->Append(3).ok());
	REQUIRE(values_builder->AppendNull().ok());
	REQUIRE(outer_builder.AppendNull().ok());

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(outer_builder.Finish(&arrow_array).ok());

	auto type = LogicalType::LIST(LogicalType::LIST(LogicalType::INTEGER));
	REQUIRE(ArrowTypeToDuckDBType(arrow_array->type()) == type);
	Vector result(type, 2);
	ConvertArrowArrayToDuckDBVector(arrow_array, result, type, 2);

	auto outer_entries = FlatVector::GetData<list_entry_t>(result);
	REQUIRE(outer_entries[0].length == 2);
	REQUIRE(!FlatVector::Validity(result).RowIsValid(1));

	auto &inner_vector = ListVector::GetEntry(result);
	auto inner_entries = FlatVector::GetData<list_entry_t>(inner_vector);
	REQUIRE(inner_entries[0].length == 2);
	REQUIRE(inner_entries[1].length == 2);

	auto &values_vector = ListVector::GetEntry(inner_vector);
	auto values = FlatVector::GetData<int32_t>(values_vector);
	REQUIRE(values[0] == 1);
	REQUIRE(values[1] == 2);
	REQUIRE(values[2] == 3);
	REQUIRE(!FlatVector::Validity(values_vector).RowIsValid(3));
}

TEST_CASE("Arrow fixed-size LIST to DuckDB ARRAY conversion", "[arrow][array]") {
	auto values_builder = std::make_shared<arrow::Int32Builder>();
	arrow::FixedSizeListBuilder array_builder(arrow::default_memory_pool(), values_builder, 3);

	REQUIRE(array_builder.Append().ok());
	REQUIRE(values_builder->Append(1).ok());
	REQUIRE(values_builder->Append(2).ok());
	REQUIRE(values_builder->Append(3).ok());
	REQUIRE(array_builder.Append().ok());
	REQUIRE(values_builder->Append(4).ok());
	REQUIRE(values_builder->AppendNull().ok());
	REQUIRE(values_builder->Append(6).ok());

	std::shared_ptr<arrow::Array> arrow_array;
	REQUIRE(array_builder.Finish(&arrow_array).ok());

	auto type = LogicalType::ARRAY(LogicalType::INTEGER, 3);
	REQUIRE(ArrowTypeToDuckDBType(arrow_array->type()) == type);

	Vector result(type, 2);
	ConvertArrowArrayToDuckDBVector(arrow_array, result, type, 2);

	auto &child_vector = ArrayVector::GetEntry(result);
	auto values = FlatVector::GetData<int32_t>(child_vector);
	REQUIRE(values[0] == 1);
	REQUIRE(values[1] == 2);
	REQUIRE(values[2] == 3);
	REQUIRE(values[3] == 4);
	REQUIRE(!FlatVector::Validity(child_vector).RowIsValid(4));
	REQUIRE(values[5] == 6);
}
