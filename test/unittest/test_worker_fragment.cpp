#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/driver/worker_node_client.hpp"
#include "server/object_storage_database.hpp"
#include "server/worker/row_group_range_scan.hpp"
#include "server/worker/worker_node.hpp"
#include "utils/network_utils.hpp"

#include <algorithm>
#include <arrow/array.h>
#include <chrono>
#include <filesystem>

using namespace duckdb; // NOLINT

namespace {

LogicalGet &FindScan(LogicalOperator &plan) {
	auto *op = &plan;
	while (op->children.size() == 1) {
		op = op->children[0].get();
	}
	REQUIRE(op->type == LogicalOperatorType::LOGICAL_GET);
	return op->Cast<LogicalGet>();
}

// Returns the optimized plan of `sql`, as optimizer extensions see it. Unlike `Connection::ExtractPlan`, its column
// references are not resolved to positions, so it can be optimized again.
unique_ptr<LogicalOperator> OptimizePlan(Connection &con, const string &sql) {
	unique_ptr<LogicalOperator> plan;
	auto statements = con.ExtractStatements(sql);
	con.context->RunFunctionInTransaction([&]() {
		Planner planner(*con.context);
		planner.CreatePlan(std::move(statements[0]));
		Optimizer optimizer(*planner.binder, *con.context);
		plan = optimizer.Optimize(std::move(planner.plan));
	});
	return plan;
}

std::filesystem::path CreateRoot(const string &name) {
	auto root =
	    std::filesystem::temp_directory_path() /
	    StringUtil::Format("%s_%s", name, std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	std::filesystem::create_directories(root);
	return root;
}

// A served database with a client connection that cuts worker fragments, as `ServedDatabase` sets it up.
struct FragmentFixture {
	explicit FragmentFixture(const std::filesystem::path &root) {
		config.set_database_uri("duckdb_objfs://fragment.db");
		config.mutable_local()->set_root(root.string());
		auto database_result = ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE);
		REQUIRE(database_result.ok());
		database = std::move(database_result).ValueOrDie();
		RegisterWorkerFragments(DBConfig::GetConfig(*database->GetInstance().instance));
		manager = make_uniq<WorkerManager>(database->GetInstance());
		auto client_result = database->Connect();
		auto executor_result = database->Connect();
		REQUIRE(client_result.ok());
		REQUIRE(executor_result.ok());
		client = std::move(client_result).ValueOrDie();
		executor_connection = std::move(executor_result).ValueOrDie();
		executor = make_uniq<DistributedExecutor>(*manager, *executor_connection, config);
		state = make_shared_ptr<WorkerFragmentState>(*executor, *executor_connection);
		client->context->registered_state->Insert(WorkerFragmentState::NAME, state);
	}

	// Runs sql on the client and requires the result to equal local execution on the executor's connection.
	vector<QueryExecutionInfo> RequireLocalResult(const string &sql) {
		INFO(sql);
		auto expected = executor_connection->Query(sql);
		REQUIRE_FALSE(expected->HasError());
		auto actual = client->Query(sql);
		INFO((actual->HasError() ? actual->GetError() : ""));
		REQUIRE_FALSE(actual->HasError());
		REQUIRE(actual->Equals(*expected));
		return state->TakeExecutions();
	}

	distributed::StorageConfig config;
	unique_ptr<ObjectStorageDatabase> database;
	unique_ptr<WorkerManager> manager;
	unique_ptr<Connection> client;
	unique_ptr<Connection> executor_connection;
	unique_ptr<DistributedExecutor> executor;
	shared_ptr<WorkerFragmentState> state;
};

} // namespace

TEST_CASE("Task plans read contiguous whole row groups up to the end of the table", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_task_plans");
	{
		DuckDB writer_db(nullptr);
		Connection writer(writer_db);
		REQUIRE_FALSE(writer.Query("LOAD duckdb_object_storage")->HasError());
		REQUIRE_FALSE(writer.Query("SET duckdb_objfs_backend = 'local'")->HasError());
		REQUIRE_FALSE(writer.Query(StringUtil::Format("SET duckdb_objfs_root = '%s'", root.string()))->HasError());
		// Small row groups give a multi-row-group table without writing 122,880 rows per group.
		REQUIRE_FALSE(
		    writer.Query("ATTACH 'duckdb_objfs://partition.db' AS object_db (ROW_GROUP_SIZE 2048)")->HasError());
		REQUIRE_FALSE(writer.Query("CREATE TABLE object_db.t AS SELECT i AS id FROM range(10000) t(i)")->HasError());
		// Deleted rows leave a gap in the rowids, which the tasks must still cover.
		REQUIRE_FALSE(writer.Query("DELETE FROM object_db.t WHERE id >= 3000 AND id < 5000")->HasError());
	}
	{
		DuckDB reader_db(nullptr);
		OptimizerExtension::Register(DBConfig::GetConfig(*reader_db.instance), GetRowGroupRangeScanExtension());
		Connection reader(reader_db);
		REQUIRE_FALSE(reader.Query("LOAD duckdb_object_storage")->HasError());
		REQUIRE_FALSE(reader.Query("SET duckdb_objfs_backend = 'local'")->HasError());
		REQUIRE_FALSE(reader.Query(StringUtil::Format("SET duckdb_objfs_root = '%s'", root.string()))->HasError());
		REQUIRE_FALSE(reader.Query("ATTACH 'duckdb_objfs://partition.db' AS object_db (READ_ONLY)")->HasError());

		auto plan = OptimizePlan(reader, "SELECT id FROM object_db.t WHERE id % 3 = 0");
		REQUIRE(plan != nullptr);
		vector<idx_t> row_group_starts;
		vector<string> task_plans;
		vector<string> single_plan;
		reader.context->RunFunctionInTransaction([&]() {
			auto &get = FindScan(*plan);
			auto row_groups = get.GetTable()->GetStorage().GetRowGroupCollection()->GetRowGroups();
			for (auto segment = row_groups->GetRootSegment(); segment; segment = row_groups->GetNextSegment(*segment)) {
				row_group_starts.emplace_back(segment->GetRowStart());
			}
			task_plans = SerializeTaskPlans(*reader.context, *plan, get, 3);
			single_plan = SerializeTaskPlans(*reader.context, *plan, get, 1);
		});
		REQUIRE(row_group_starts.size() >= 3);
		REQUIRE(task_plans.size() == 3);
		REQUIRE(single_plan.size() == 1);

		auto expected = reader.Query("SELECT count(*) FROM object_db.t WHERE id % 3 = 0")->GetValue(0, 0);
		idx_t next_rowid = 0;
		idx_t rows = 0;
		for (idx_t task_idx = 0; task_idx < task_plans.size(); ++task_idx) {
			auto task = PlanSerializer::DeserializeLogicalPlan(*reader.context, task_plans[task_idx]);
			// Each task starts where the previous one ends, at a row group start; the last one has no end.
			auto &filters = FindScan(*task).table_filters.filters;
			auto entry = filters.find(COLUMN_IDENTIFIER_ROW_ID);
			REQUIRE(entry != filters.end());
			auto &range = entry->second->Cast<ConjunctionAndFilter>().child_filters;
			const bool last = task_idx + 1 == task_plans.size();
			REQUIRE(range.size() == (last ? 1 : 2));
			REQUIRE(range[0]->Cast<ConstantFilter>().constant.GetValue<idx_t>() == next_rowid);
			if (!last) {
				next_rowid = range[1]->Cast<ConstantFilter>().constant.GetValue<idx_t>();
				REQUIRE(std::find(row_group_starts.begin(), row_group_starts.end(), next_rowid) !=
				        row_group_starts.end());
			}
			auto result = reader.Query(make_uniq<LogicalPlanStatement>(std::move(task)));
			INFO((result->HasError() ? result->GetError() : ""));
			REQUIRE_FALSE(result->HasError());
			rows += result->RowCount();
		}
		REQUIRE(Value::UBIGINT(rows) == expected.DefaultCastAs(LogicalType::UBIGINT));

		auto whole = PlanSerializer::DeserializeLogicalPlan(*reader.context, single_plan[0]);
		REQUIRE(FindScan(*whole).table_filters.filters.count(COLUMN_IDENTIFIER_ROW_ID) == 0);
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Optimizer sends a two-table Join aggregate through a worker fragment", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_join_fragment");
	{
		FragmentFixture fixture(root);
		auto &client = fixture.client;
		auto &executor_connection = fixture.executor_connection;
		auto &state = fixture.state;
		REQUIRE_FALSE(executor_connection
		                  ->Query("CREATE TABLE fact AS SELECT i AS id, i % 17 AS k, i % 11 AS amount, "
		                          "CASE WHEN i % 13 = 0 THEN NULL ELSE i % 4 END AS g, "
		                          "CAST(i % 101 AS DECIMAL(15, 2)) AS price FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(
		    executor_connection->Query("CREATE TABLE dim AS SELECT i AS k, i + 1 AS multiplier FROM range(17) t(i)")
		        ->HasError());
		REQUIRE_FALSE(executor_connection->Query("CREATE SCHEMA alt")->HasError());
		REQUIRE_FALSE(executor_connection
		                  ->Query("CREATE TABLE alt.fact AS SELECT i AS id, i % 17 AS k, 2 AS amount "
		                          "FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(
		    executor_connection->Query("CREATE TABLE alt.dim AS SELECT i AS k, 10 AS multiplier FROM range(17) t(i)")
		        ->HasError());

		// Without workers nothing is cut.
		const string sql = "SELECT sum(f.amount * d.multiplier) AS revenue FROM fact f "
		                   "JOIN dim d ON f.k = d.k WHERE (d.k = 2 AND f.amount < 5) "
		                   "OR (d.k = 3 AND f.amount >= 5)";
		REQUIRE(fixture.RequireLocalResult(sql).empty());

		// Each worker optimizes its plan again, deriving d.k IN (2, 3) from the OR for dim before the Join.
		REQUIRE(fixture.manager->StartLocalWorkers(2).ok());
		auto executions = fixture.RequireLocalResult(sql);
		REQUIRE(executions.size() == 1);
		REQUIRE(executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		REQUIRE(executions[0].merge_strategy == MergeStrategy::AGGREGATE_MERGE);
		REQUIRE(executions[0].num_tasks_generated == 2);
		REQUIRE(executions[0].sql ==
		        "aggregate sum((amount * multiplier)) over object_db.main.fact (partitioned), object_db.main.dim");

		// Only the aggregate becomes a fragment; the operators above it run on the driver, so names, types and order
		// match local execution.
		for (const auto &shape_sql :
		     {// GROUP BY a column, CASE inside the aggregates, IN, ORDER BY.
		      "SELECT f.g, sum(CASE WHEN d.k IN (1, 2) OR d.k IS NULL THEN 1 ELSE 0 END) AS high, "
		      "sum(CASE WHEN NOT (d.k < 5) THEN 1 ELSE 0 END) AS low, count(*) AS n FROM fact f, dim d "
		      "WHERE f.k = d.k AND f.amount BETWEEN 2 AND 8 GROUP BY f.g ORDER BY f.g",
		      // Arithmetic over aggregates.
		      "SELECT 100.00 * sum(CASE WHEN d.k % 2 = 0 THEN f.price * d.multiplier ELSE 0 END) / "
		      "sum(f.price * d.multiplier) AS promo FROM fact f JOIN dim d ON f.k = d.k",
		      "SELECT avg(f.price) AS mean, avg(f.amount), d.multiplier % 3 AS bucket FROM fact f JOIN dim d "
		      "ON f.k = d.k GROUP BY d.multiplier % 3 HAVING count(*) > 0 ORDER BY sum(f.amount) DESC LIMIT 2",
		      // Empty input: an ungrouped count is 0 and a sum NULL.
		      "SELECT count(*), sum(f.price), count(*) * 2 FROM fact f JOIN dim d ON f.k = d.k "
		      "WHERE f.id::VARCHAR = 'x'"}) {
			auto shape_executions = fixture.RequireLocalResult(shape_sql);
			REQUIRE(shape_executions.size() == 1);
			REQUIRE(shape_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
			REQUIRE(shape_executions[0].num_tasks_generated == 2);
		}
		// Shapes whose partial results cannot be merged, or that are not inner Joins, run locally.
		for (const auto &fallback_sql :
		     {"SELECT count(DISTINCT f.amount) FROM fact f JOIN dim d ON f.k = d.k",
		      "SELECT median(f.amount) FROM fact f JOIN dim d ON f.k = d.k",
		      "SELECT sum(f.amount) FILTER (WHERE d.k > 3) FROM fact f JOIN dim d ON f.k = d.k",
		      "SELECT f.g, sum(f.amount) FROM fact f JOIN dim d ON f.k = d.k GROUP BY ROLLUP (f.g) ORDER BY ALL",
		      "SELECT sum(f.amount) FROM fact f LEFT JOIN dim d ON f.k = d.k AND d.k < 3"}) {
			REQUIRE(fixture.RequireLocalResult(fallback_sql).empty());
		}
		// Macros are expanded before optimization, so a macro shadowing sum is distributed as what it computes.
		REQUIRE_FALSE(client->Query("CREATE TEMP MACRO sum(x) AS max(x)")->HasError());
		auto macro_result = client->Query("SELECT sum(f.amount), count(*) FROM fact f JOIN dim d ON f.k = d.k");
		REQUIRE_FALSE(macro_result->HasError());
		REQUIRE(macro_result->GetValue(0, 0) == Value::BIGINT(10));
		REQUIRE(macro_result->GetValue(1, 0) == Value::BIGINT(300000));
		REQUIRE(state->TakeExecutions().size() == 1);
		REQUIRE_FALSE(client->Query("DROP MACRO temp.sum")->HasError());

		// One row group on each side yields one task, so there is no Join work to spread across workers.
		REQUIRE_FALSE(executor_connection->Query("CREATE TABLE dim2 AS SELECT * FROM dim")->HasError());
		REQUIRE(fixture.RequireLocalResult("SELECT sum(d.multiplier) FROM dim d JOIN dim2 e ON d.k = e.k").empty());

		REQUIRE_FALSE(client->Query("CREATE TEMP TABLE temp_dim AS SELECT 2 AS k, 3 AS multiplier")->HasError());
		auto temp_result =
		    client->Query("SELECT sum(f.amount * d.multiplier) FROM fact f JOIN temp_dim d ON f.k = d.k");
		REQUIRE_FALSE(temp_result->HasError());
		REQUIRE(state->TakeExecutions().empty());

		auto scan_executions = fixture.RequireLocalResult("SELECT sum(amount) FROM fact");
		REQUIRE(scan_executions.size() == 1);
		REQUIRE(scan_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);

		// Single-table fragments, including expressions that could not be rendered as SQL, run on workers. NULL inputs
		// must keep local semantics.
		for (const auto &single_sql :
		     {"SELECT sum(CASE WHEN amount > 5 THEN price ELSE 0 END), count(*) FROM fact",
		      "SELECT sum(CASE WHEN k IN (1, 2, 3) THEN 1 ELSE 0 END), "
		      "count(CASE WHEN amount BETWEEN 2 AND 4 THEN 1 END), "
		      "sum(CASE WHEN g IS NULL THEN amount END), max(amount >= 7 AND NOT (k < 3)) FROM fact",
		      "SELECT CASE WHEN g IS NULL THEN 'none' WHEN g < 2 THEN 'low' ELSE 'high' END AS bucket, sum(amount) "
		      "FROM fact GROUP BY bucket ORDER BY bucket",
		      "SELECT count(*), sum(amount) FROM fact WHERE id::VARCHAR = '123457'",
		      "SELECT count(*), sum(amount) FROM fact WHERE id::VARCHAR = 'x'",
		      "SELECT id, amount FROM fact WHERE id::VARCHAR = '123457'",
		      // NULL semantics: NOT IN with a NULL in the list, CASE without ELSE, BETWEEN with a NULL bound, and
		      // comparisons with NULL columns or constants.
		      "SELECT count(CASE WHEN g NOT IN (1, NULL) THEN 1 END), count(CASE WHEN g NOT IN (1, 2) THEN 1 END), "
		      "count(CASE WHEN g IN (1, NULL) THEN 1 END), sum(CASE WHEN g = 1 THEN amount END) FROM fact",
		      "SELECT count(CASE WHEN g BETWEEN NULL AND 3 THEN 1 END), "
		      "count(CASE WHEN g BETWEEN 1 AND NULL THEN 1 END), "
		      "count(CASE WHEN g = NULL THEN 1 END), count(CASE WHEN g IS DISTINCT FROM 1 THEN 1 END), "
		      "count(CASE WHEN NOT (g > 1) THEN 1 END), count(CASE WHEN g IS NOT NULL THEN 1 END) FROM fact",
		      "SELECT g, count(*), sum(CASE WHEN g <> 2 OR g IS NULL THEN amount END) FROM fact "
		      "WHERE g IS NULL OR g <> 2 GROUP BY g ORDER BY g",
		      "SELECT sum(COALESCE(g, 0)) FROM fact", "SELECT max([amount, k]) FROM fact"}) {
			auto single_executions = fixture.RequireLocalResult(single_sql);
			REQUIRE(single_executions.size() == 1);
			REQUIRE(single_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		}
		// The executor's connection cannot see the client's temporary tables, so they are read locally.
		REQUIRE_FALSE(client->Query("CREATE TEMP TABLE temp_fact AS SELECT * FROM fact")->HasError());
		// The CREATE's own scan of fact may run on workers.
		state->TakeExecutions();
		auto temp_scan_result = client->Query("SELECT sum(amount), count(*) FROM temp_fact");
		REQUIRE_FALSE(temp_scan_result->HasError());
		REQUIRE(temp_scan_result->GetValue(1, 0) == Value::BIGINT(300000));
		REQUIRE(state->TakeExecutions().empty());

		REQUIRE_FALSE(client->Query("BEGIN TRANSACTION")->HasError());
		REQUIRE_FALSE(client->Query(sql)->HasError());
		REQUIRE(state->TakeExecutions().empty());
		REQUIRE_FALSE(client->Query("ROLLBACK")->HasError());

		// Plans resolve tables by the catalog and schema the client bound them in.
		REQUIRE_FALSE(client->Query("USE alt")->HasError());
		const string schema_sql = "SELECT sum(f.amount * d.multiplier) AS revenue FROM fact f JOIN dim d ON f.k = d.k";
		auto schema_expected = executor_connection->Query(
		    "SELECT sum(f.amount * d.multiplier) AS revenue FROM alt.fact f JOIN alt.dim d ON f.k = d.k");
		auto default_result = executor_connection->Query(schema_sql);
		REQUIRE_FALSE(schema_expected->HasError());
		REQUIRE_FALSE(default_result->HasError());
		REQUIRE(schema_expected->GetValue(0, 0) != default_result->GetValue(0, 0));
		auto schema_actual = client->Query(schema_sql);
		REQUIRE_FALSE(schema_actual->HasError());
		REQUIRE(schema_actual->Equals(*schema_expected));
		auto schema_executions = state->TakeExecutions();
		REQUIRE(schema_executions.size() == 1);
		REQUIRE(schema_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		REQUIRE(schema_executions[0].num_tasks_generated == 2);
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Join fragments only replicate inputs that are cheap enough", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_join_cost");
	{
		FragmentFixture fixture(root);
		auto &executor_connection = fixture.executor_connection;
		// wide has more rows than fact, but few of them pass an equality filter on g.
		REQUIRE_FALSE(executor_connection
		                  ->Query("CREATE TABLE fact AS SELECT i AS id, i % 1000 AS k, i % 11 AS amount, "
		                          "i % 7 AS c FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(executor_connection
		                  ->Query("CREATE TABLE wide AS SELECT i AS id, i % 1000 AS k, i % 1000 AS g "
		                          "FROM range(600000) t(i)")
		                  ->HasError());
		REQUIRE(fixture.manager->StartLocalWorkers(2).ok());

		// The filtered wide is the cheaper input, so fact is partitioned although it has fewer rows.
		auto executions = fixture.RequireLocalResult(
		    "SELECT sum(f.amount * f.c), count(*) FROM fact f JOIN wide w ON f.k = w.k WHERE w.g = 3");
		REQUIRE(executions.size() == 1);
		REQUIRE(executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		REQUIRE(executions[0].num_tasks_generated == 2);
		INFO(executions[0].sql);
		REQUIRE(StringUtil::Contains(executions[0].sql, "object_db.main.fact (partitioned)"));

		// Unfiltered, every worker would read and build all of the other input, which costs more than it saves.
		executions = fixture.RequireLocalResult("SELECT sum(f.amount), count(*) FROM fact f JOIN wide w ON f.k = w.k");
		REQUIRE(executions.empty());
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Worker fragments only read tables of the served catalog", "[worker_fragment][catalog]") {
	auto root = CreateRoot("duckherder_catalog_fragment");
	{
		FragmentFixture fixture(root);
		auto &client = fixture.client;
		auto &state = fixture.state;
		REQUIRE_FALSE(fixture.executor_connection
		                  ->Query("CREATE TABLE fact AS SELECT i AS id, i % 17 AS k, 2 AS amount "
		                          "FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(
		    fixture.executor_connection->Query("CREATE TABLE dim AS SELECT i AS k, 5 AS multiplier FROM range(17) t(i)")
		        ->HasError());
		REQUIRE(fixture.manager->StartLocalWorkers(2).ok());

		// Workers only attach the served database, so a plan over another catalog's table would fail to resolve there,
		// or resolve a same-named table of the worker's own catalogs.
		REQUIRE_FALSE(client->Query("ATTACH ':memory:' AS \"other catalog\"")->HasError());
		REQUIRE_FALSE(client
		                  ->Query("CREATE TABLE \"other catalog\".main.fact AS SELECT i AS id, i % 17 AS k, "
		                          "3 AS amount FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(client
		                  ->Query("CREATE TABLE \"other catalog\".main.dim AS "
		                          "SELECT i AS k, 7 AS multiplier FROM range(17) t(i)")
		                  ->HasError());
		for (const auto &foreign_sql : {"SELECT sum(f.amount * d.multiplier) FROM \"other catalog\".main.fact f "
		                                "JOIN \"other catalog\".main.dim d ON f.k = d.k",
		                                "SELECT sum(amount) FROM \"other catalog\".main.fact",
		                                "SELECT amount FROM \"other catalog\".main.fact WHERE id = 1",
		                                "SELECT sum(f.amount * d.multiplier) FROM \"other catalog\".main.fact f "
		                                "JOIN object_db.main.dim d ON f.k = d.k",
		                                "SELECT sum(f.amount * d.multiplier) FROM object_db.main.fact f "
		                                "JOIN \"other catalog\".main.dim d ON f.k = d.k"}) {
			REQUIRE(fixture.RequireLocalResult(foreign_sql).empty());
		}

		// Changing the client's default catalog does not change which database the executor serves.
		REQUIRE_FALSE(client->Query("USE \"other catalog\"")->HasError());
		auto foreign_default = client->Query("SELECT sum(f.amount * d.multiplier) FROM fact f JOIN dim d ON f.k = d.k");
		REQUIRE_FALSE(foreign_default->HasError());
		REQUIRE(foreign_default->GetValue(0, 0).GetValue<int64_t>() == 6300000);
		REQUIRE(state->TakeExecutions().empty());

		// Fully qualified served tables still produce partitioned tasks after USE.
		auto served_executions =
		    fixture.RequireLocalResult("SELECT sum(f.amount * d.multiplier) FROM object_db.main.fact f "
		                               "JOIN object_db.main.dim d ON f.k = d.k");
		REQUIRE(served_executions.size() == 1);
		REQUIRE(served_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		REQUIRE(served_executions[0].num_tasks_generated == 2);
		REQUIRE(served_executions[0].sql ==
		        "aggregate sum((amount * multiplier)) over object_db.main.fact (partitioned), object_db.main.dim");

		// Multi-table Joins only distribute when every table belongs to the served catalog.
		const string three_table_sql = "SELECT sum(f.amount * d.multiplier * o.multiplier) FROM object_db.main.fact f "
		                               "JOIN object_db.main.dim d ON f.k = d.k JOIN %s.main.dim o ON f.k = o.k";
		REQUIRE(fixture.RequireLocalResult(StringUtil::Format(three_table_sql, "\"other catalog\"")).empty());
		auto multi_executions = fixture.RequireLocalResult(StringUtil::Format(three_table_sql, "object_db"));
		REQUIRE(multi_executions.size() == 1);
		REQUIRE(multi_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Driver merges partial aggregates exactly", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_merge");
	{
		FragmentFixture fixture(root);
		REQUIRE_FALSE(
		    fixture.executor_connection
		        ->Query("CREATE TABLE agg AS SELECT i AS id, CASE WHEN i % 7 = 0 THEN NULL ELSE i % 5 END AS g, "
		                "CAST(i % 300 AS SMALLINT) AS s, CAST(i * 7 AS INTEGER) AS n, "
		                "CAST(i AS BIGINT) * 1000003 AS b, CAST(i AS HUGEINT) * 100000000000 AS h, "
		                "CAST(i % 1000 / 10 AS DECIMAL(4, 1)) AS d4, CAST(i * 1.37 AS DECIMAL(15, 2)) AS d15, "
		                "CAST(i * 3.141 AS DECIMAL(38, 3)) AS d38, CAST(i AS DOUBLE) / 10 AS f, "
		                "CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS maybe FROM range(300000) t(i)")
		        ->HasError());
		REQUIRE(fixture.manager->StartLocalWorkers(3).ok());
		const string aggregates = "sum(n), sum(d15), min(s), max(d38), min(g), count(maybe), count(*), avg(s), "
		                          "avg(n), avg(b), avg(h), avg(d4), avg(d15), avg(d38), avg(maybe)";
		for (const auto &sql :
		     {StringUtil::Format("SELECT g, %s FROM agg GROUP BY g ORDER BY g", aggregates),
		      StringUtil::Format("SELECT %s FROM agg", aggregates),
		      // Empty input: counts are 0, and the other aggregates NULL.
		      StringUtil::Format("SELECT %s FROM agg WHERE id::VARCHAR = 'x'", aggregates),
		      StringUtil::Format("SELECT g, %s FROM agg WHERE id::VARCHAR = 'x' GROUP BY g", aggregates)}) {
			auto executions = fixture.RequireLocalResult(sql);
			REQUIRE(executions.size() == 1);
			REQUIRE(executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
			REQUIRE(executions[0].num_tasks_generated == 3);
			REQUIRE(StringUtil::StartsWith(executions[0].sql, "aggregate "));
		}
		// avg over DOUBLE runs on the driver because floating-point sums depend on row order. So do aggregates
		// without a merge, such as median and the top-N max(x, n). Workers only scan for them.
		for (const auto &sql : {"SELECT avg(f) FROM agg", "SELECT g, median(n) FROM agg GROUP BY g ORDER BY g",
		                        "SELECT max(n, 3), min(s, 2) FROM agg"}) {
			auto executions = fixture.RequireLocalResult(sql);
			REQUIRE(executions.size() == 1);
			INFO(executions[0].sql);
			REQUIRE(StringUtil::StartsWith(executions[0].sql, "scan over object_db.main.agg"));
		}
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Only scans every output row comes from one row of are partitioned", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_partition_safety");
	{
		FragmentFixture fixture(root);
		auto &executor_connection = fixture.executor_connection;
		// Each third of skew has a different amount, so the average of a part differs from the whole table's.
		REQUIRE_FALSE(executor_connection
		                  ->Query("CREATE TABLE skew AS SELECT i AS id, i % 17 AS k, i // 100000 AS amount "
		                          "FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(
		    executor_connection->Query("CREATE TABLE dim AS SELECT i AS k, i + 1 AS multiplier FROM range(17) t(i)")
		        ->HasError());
		REQUIRE_FALSE(
		    executor_connection->Query("CREATE TABLE dim2 AS SELECT i AS k, i % 3 AS bucket FROM range(17) t(i)")
		        ->HasError());
		REQUIRE(fixture.manager->StartLocalWorkers(3).ok());

		// The correlated subquery becomes a LEFT Join of skew with an aggregate over skew,
		// so no scan is partitioned.
		const string correlated_sql = "SELECT sum(s.amount), count(*) FROM skew s, dim d WHERE s.k = d.k AND "
		                              "s.amount > (SELECT avg(s2.amount) FROM skew s2 WHERE s2.k = s.k)";
		// The same with an inner Join: only the outer skew may be partitioned, and every worker must average all of
		// the inner one.
		const string aggregate_join_sql = "SELECT sum(s.amount), count(*) FROM skew s JOIN (SELECT k, avg(amount) AS a "
		                                  "FROM skew GROUP BY k) t ON s.k = t.k WHERE s.amount > t.a";
		const string three_table_sql =
		    "SELECT e.bucket, sum(s.amount * d.multiplier), count(*) FROM skew s "
		    "JOIN dim d ON s.k = d.k JOIN dim2 e ON d.k = e.k GROUP BY e.bucket ORDER BY ALL";
		for (const auto &sql : {correlated_sql, aggregate_join_sql}) {
			REQUIRE(fixture.RequireLocalResult(sql).empty());
		}
		auto executions = fixture.RequireLocalResult(three_table_sql);
		REQUIRE(executions.size() == 1);
		REQUIRE(executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		REQUIRE(executions[0].num_tasks_generated == 3);
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Workers reject plans of another build and the driver runs them locally", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_build_mismatch");
	{
		FragmentFixture fixture(root);
		REQUIRE_FALSE(
		    fixture.executor_connection->Query("CREATE TABLE t AS SELECT i AS id, i % 7 AS k FROM range(300000) t(i)")
		        ->HasError());
		auto plan = OptimizePlan(*fixture.executor_connection, "SELECT count(*), max(id) FROM t");
		REQUIRE(plan != nullptr);

		WorkerNode worker("mismatch_worker", "localhost", GetAvailablePort(9500));
		REQUIRE(worker.Start().ok());
		WorkerNodeClient worker_client(worker.GetLocation());
		REQUIRE(worker_client.Connect().ok());
		distributed::ExecutePartitionRequest request;
		request.set_serialized_plan(PlanSerializer::SerializeLogicalPlan(*plan));
		*request.mutable_storage_config() = fixture.config;
		request.set_build_id("other/0.0.0");
		arrow::RecordBatchVector batches;
		auto status = worker_client.ExecutePartition(request, batches);
		REQUIRE_FALSE(status.ok());
		REQUIRE(StringUtil::Contains(status.ToString(), "cannot run on worker mismatch_worker"));
		request.set_build_id(PlanSerializer::BuildId());
		REQUIRE(worker_client.ExecutePartition(request, batches).ok());
		REQUIRE(batches.size() == 1);
		REQUIRE(batches[0]->num_rows() == 1);
		REQUIRE(batches[0]->column(0)->type_id() == arrow::Type::INT64);
		REQUIRE(batches[0]->column(1)->type_id() == arrow::Type::INT64);
		REQUIRE(std::static_pointer_cast<arrow::Int64Array>(batches[0]->column(0))->Value(0) == 300000);
		REQUIRE(std::static_pointer_cast<arrow::Int64Array>(batches[0]->column(1))->Value(0) == 299999);
		worker.Shutdown();

		// A task that fails on its worker, whatever the reason, makes the driver run the fragment itself.
		REQUIRE(fixture.manager->RegisterWorker("unreachable", "grpc://localhost:1").ok());
		auto executions = fixture.RequireLocalResult("SELECT k, sum(id), avg(id) FROM t GROUP BY k ORDER BY k");
		REQUIRE(executions.size() == 1);
		REQUIRE(executions[0].execution_mode == QueryExecutionMode::LOCAL);
		REQUIRE(StringUtil::StartsWith(executions[0].sql, "aggregate "));
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Prepared fragments read rows appended before they execute", "[worker_fragment]") {
	auto root = CreateRoot("duckherder_appended");
	{
		FragmentFixture fixture(root);
		auto &executor_connection = fixture.executor_connection;
		REQUIRE_FALSE(executor_connection
		                  ->Query("CREATE TABLE fact AS SELECT i AS id, i % 17 AS k, i % 11 AS amount "
		                          "FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(
		    executor_connection->Query("CREATE TABLE dim AS SELECT i AS k, i + 1 AS multiplier FROM range(17) t(i)")
		        ->HasError());
		// Workers attach their snapshot of the database at their first task, after the INSERT below.
		REQUIRE(fixture.manager->StartLocalWorkers(2).ok());
		const vector<string> queries {"SELECT k, count(*), sum(amount) FROM fact GROUP BY k ORDER BY k",
		                              "SELECT d.multiplier % 3 AS bucket, sum(f.amount * d.multiplier), count(*) "
		                              "FROM fact f JOIN dim d ON f.k = d.k GROUP BY bucket ORDER BY bucket"};
		vector<unique_ptr<PreparedStatement>> prepared;
		for (const auto &sql : queries) {
			prepared.emplace_back(fixture.client->Prepare(sql));
			REQUIRE_FALSE(prepared.back()->HasError());
		}
		// The task ranges were planned for three row groups; the INSERT adds three more. Its keys are outside the range
		// the plan was optimized for, which a plan compressed with the driver's statistics would wrap.
		REQUIRE_FALSE(
		    executor_connection->Query("INSERT INTO fact SELECT i, i % 1000, i % 7 FROM range(300000, 600000) t(i)")
		        ->HasError());
		REQUIRE_FALSE(executor_connection->Query("CHECKPOINT")->HasError());
		for (idx_t idx = 0; idx < queries.size(); ++idx) {
			INFO(queries[idx]);
			auto expected = executor_connection->Query(queries[idx]);
			REQUIRE_FALSE(expected->HasError());
			vector<Value> parameters;
			auto actual = prepared[idx]->Execute(parameters, /*allow_stream_result=*/false);
			REQUIRE_FALSE(actual->HasError());
			REQUIRE(actual->Equals(*expected));
			auto executions = fixture.state->TakeExecutions();
			REQUIRE(executions.size() == 1);
			REQUIRE(executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
			REQUIRE(executions[0].num_tasks_generated == 2);
		}
	}
	std::filesystem::remove_all(root);
}
