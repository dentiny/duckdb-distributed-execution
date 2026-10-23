#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb; // NOLINT

// Workers split a table into rowid ranges of whole row groups. DuckDB's table scan must skip the row groups outside a
// task's range, or every task reads the whole table.
TEST_CASE("Table scans skip the row groups outside a rowid range", "[row_group_range_scan]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE t AS SELECT i AS id FROM range(5 * 122880) t(i)")->HasError());

	// Row groups 2 and 3, the range a worker task gets.
	auto result = con.Query("EXPLAIN ANALYZE SELECT count(id) FROM t WHERE rowid >= 245760 AND rowid < 491520");
	REQUIRE_FALSE(result->HasError());
	const auto plan = result->Collection().GetValue(1, 0).ToString();
	INFO(plan);
	REQUIRE(StringUtil::Contains(plan, "Row Groups Scanned: 2 / 5"));
}
