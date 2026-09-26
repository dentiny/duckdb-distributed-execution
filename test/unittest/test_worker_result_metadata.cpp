#include "catch/catch.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/query_result.hpp"
#include "server/worker/worker_result_metadata.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Worker result uses coordinator logical metadata", "[worker][types]") {
	vector<LogicalType> worker_types {LogicalType::BIGINT};
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), worker_types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), worker_types);
	chunk.SetValue(0, 0, Value::BIGINT(42));
	chunk.SetCardinality(1);
	collection->Append(chunk);

	auto worker_result = make_uniq<QueryResult>(StatementType::SELECT_STATEMENT, StatementProperties {},
	                                            vector<Identifier> {Identifier("worker_value")}, std::move(collection),
	                                            ClientProperties {});
	vector<LogicalType> coordinator_types {LogicalType::DECIMAL(10, 2)};
	vector<string> coordinator_names {"coordinator_value"};

	auto result = ApplyCoordinatorResultMetadata(std::move(worker_result), coordinator_types, coordinator_names);

	REQUIRE(result->GetTypes() == coordinator_types);
	REQUIRE(result->GetNames()[0].GetIdentifierName() == coordinator_names[0]);
	REQUIRE(result->Collection().Types() == coordinator_types);
	REQUIRE(result->RowCount() == 1);
}
