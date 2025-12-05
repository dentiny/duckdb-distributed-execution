#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "duckdb.hpp"
#include "server/driver/query_utils.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("ContainsTableScan Tests", "[query_utils]") {
	DuckDB db(nullptr);
	Connection con(db);
	
	SECTION("Simple SELECT with TABLE_SCAN") {
		// Create a table
		con.Query("CREATE TABLE test_table (id INTEGER, value VARCHAR)");
		con.Query("INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c')");
		
		// Extract plan for a simple SELECT
		auto result = con.Query("EXPLAIN SELECT * FROM test_table");
		REQUIRE(!result->HasError());
		
		// Get the physical plan
		auto plan = con.ExtractPlan("SELECT * FROM test_table");
		REQUIRE(plan != nullptr);
		
		// Generate physical plan
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		// Test ContainsTableScan
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
	
	SECTION("SELECT with GROUP BY (TABLE_SCAN as child)") {
		// Create a table
		con.Query("CREATE TABLE group_test (category VARCHAR, value INTEGER)");
		con.Query("INSERT INTO group_test VALUES ('A', 10), ('B', 20), ('A', 30)");
		
		// Extract plan for GROUP BY query
		auto plan = con.ExtractPlan("SELECT category, SUM(value) FROM group_test GROUP BY category");
		REQUIRE(plan != nullptr);
		
		// Generate physical plan
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		// Test ContainsTableScan - should find TABLE_SCAN even though root is HASH_GROUP_BY
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
	
	SECTION("SELECT with WHERE clause") {
		// Create a table
		con.Query("CREATE TABLE filter_test (id INTEGER, status VARCHAR)");
		con.Query("INSERT INTO filter_test VALUES (1, 'active'), (2, 'inactive')");
		
		// Extract plan for filtered SELECT
		auto plan = con.ExtractPlan("SELECT * FROM filter_test WHERE status = 'active'");
		REQUIRE(plan != nullptr);
		
		// Generate physical plan
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		// Test ContainsTableScan
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
	
	SECTION("SELECT with multiple aggregates") {
		// Create a table
		con.Query("CREATE TABLE agg_test (category VARCHAR, amount INTEGER, quantity INTEGER)");
		con.Query("INSERT INTO agg_test VALUES ('X', 100, 5), ('Y', 200, 10)");
		
		// Extract plan for multiple aggregates
		auto plan = con.ExtractPlan("SELECT category, COUNT(*), SUM(amount), AVG(quantity) FROM agg_test GROUP BY category");
		REQUIRE(plan != nullptr);
		
		// Generate physical plan
		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));
		
		// Test ContainsTableScan
		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
}
