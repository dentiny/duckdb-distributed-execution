#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "duckdb.hpp"
#include "server/driver/query_utils.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("ContainsTableScan Tests", "[query_utils]") {
	DuckDB db(nullptr);
	Connection con(db);
	
	SECTION("Simple SELECT with TABLE_SCAN") {
		con.Query("CREATE TABLE test_table (id INTEGER, value VARCHAR)");
		con.Query("INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c')");
		
		auto result = con.Query("EXPLAIN SELECT * FROM test_table");
		REQUIRE(!result->HasError());
		
		auto plan = con.ExtractPlan("SELECT * FROM test_table");
		REQUIRE(plan != nullptr);
		
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
	
	SECTION("SELECT with GROUP BY (TABLE_SCAN as child)") {
		con.Query("CREATE TABLE group_test (category VARCHAR, value INTEGER)");
		con.Query("INSERT INTO group_test VALUES ('A', 10), ('B', 20), ('A', 30)");
		
		auto plan = con.ExtractPlan("SELECT category, SUM(value) FROM group_test GROUP BY category");
		REQUIRE(plan != nullptr);
		
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
	
	SECTION("SELECT with WHERE clause") {
		con.Query("CREATE TABLE filter_test (id INTEGER, status VARCHAR)");
		con.Query("INSERT INTO filter_test VALUES (1, 'active'), (2, 'inactive')");
		
		auto plan = con.ExtractPlan("SELECT * FROM filter_test WHERE status = 'active'");
		REQUIRE(plan != nullptr);
		
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
	
	SECTION("SELECT with multiple aggregates") {
		con.Query("CREATE TABLE agg_test (category VARCHAR, amount INTEGER, quantity INTEGER)");
		con.Query("INSERT INTO agg_test VALUES ('X', 100, 5), ('Y', 200, 10)");
		
		auto plan = con.ExtractPlan("SELECT category, COUNT(*), SUM(amount), AVG(quantity) FROM agg_test GROUP BY category");
		REQUIRE(plan != nullptr);
		
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
}
