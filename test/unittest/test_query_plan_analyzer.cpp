#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/task_partitioner.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>

using namespace duckdb; // NOLINT

namespace {

QueryPlanAnalyzer::QueryAnalysis AnalyzeQuery(Connection &con, const string &sql) {
	auto plan = con.ExtractPlan(sql);
	REQUIRE(plan != nullptr);
	auto statements = con.ExtractStatements(sql);
	return QueryPlanAnalyzer::AnalyzeQuery(*plan, statements[0]->Cast<SelectStatement>());
}

} // namespace

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

		auto plan =
		    con.ExtractPlan("SELECT category, COUNT(*), SUM(amount), AVG(quantity) FROM agg_test GROUP BY category");
		REQUIRE(plan != nullptr);

		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));

		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
}

TEST_CASE("Partition tasks cover deleted rowid gaps without changing WHERE precedence", "[task_partitioner]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE partition_test AS SELECT i AS id FROM range(250000) t(i)")->HasError());
	REQUIRE_FALSE(con.Query("DELETE FROM partition_test WHERE id >= 10000 AND id < 20000")->HasError());

	QueryPlanAnalyzer analyzer(con);
	TaskPartitioner partitioner(con, analyzer);
	const string limit_sql = "SELECT id FROM partition_test LIMIT 10";
	auto limit_plan = con.ExtractPlan(limit_sql);
	REQUIRE(limit_plan != nullptr);
	REQUIRE(partitioner.ExtractPipelineTasks(*limit_plan, limit_sql, 3).size() == 1);
	for (const auto workers : {3, 5}) {
		const string sql = "SELECT t.id FROM partition_test AS t WHERE t.id = 5 OR t.id = 200005";
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		auto tasks = partitioner.ExtractPipelineTasks(*plan, sql, workers);
		// Never split a row group, so extra workers get no task.
		const auto total_row_groups = analyzer.ExtractRowGroupInfo(*plan).total_row_groups;
		REQUIRE(total_row_groups > 1);
		REQUIRE(tasks.size() == std::min<idx_t>(workers, total_row_groups));
		idx_t matches = 0;
		for (const auto &task : tasks) {
			auto result = con.Query(StringUtil::Format("SELECT count(*) FROM (%s)", task.task_sql));
			REQUIRE_FALSE(result->HasError());
			matches += result->GetValue(0, 0).GetValue<idx_t>();
		}
		REQUIRE(matches == 2);

		const string full_sql = "SELECT id FROM partition_test";
		plan = con.ExtractPlan(full_sql);
		REQUIRE(plan != nullptr);
		tasks = partitioner.ExtractPipelineTasks(*plan, full_sql, workers);
		idx_t rows = 0;
		for (const auto &task : tasks) {
			auto result = con.Query(StringUtil::Format("SELECT count(*) FROM (%s)", task.task_sql));
			REQUIRE_FALSE(result->HasError());
			rows += result->GetValue(0, 0).GetValue<idx_t>();
		}
		REQUIRE(rows == 240000);
	}
}

TEST_CASE("Local ObjFS scans assign contiguous whole row groups", "[task_partitioner]") {
	auto root = std::filesystem::temp_directory_path() /
	            StringUtil::Format("duckherder_partition_%s",
	                               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	std::filesystem::create_directories(root);
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
		REQUIRE_FALSE(writer.Query("CREATE TABLE object_db.tiny AS SELECT i AS id FROM range(8) t(i)")->HasError());
	}
	{
		DuckDB reader_db(nullptr);
		Connection reader(reader_db);
		REQUIRE_FALSE(reader.Query("LOAD duckdb_object_storage")->HasError());
		REQUIRE_FALSE(reader.Query("SET duckdb_objfs_backend = 'local'")->HasError());
		REQUIRE_FALSE(reader.Query(StringUtil::Format("SET duckdb_objfs_root = '%s'", root.string()))->HasError());
		REQUIRE_FALSE(reader.Query("ATTACH 'duckdb_objfs://partition.db' AS object_db (READ_ONLY)")->HasError());
		QueryPlanAnalyzer analyzer(reader);
		TaskPartitioner partitioner(reader, analyzer);

		// A single row group is never split across workers.
		const string tiny_sql = "SELECT id FROM object_db.tiny";
		auto plan = reader.ExtractPlan(tiny_sql);
		REQUIRE(plan != nullptr);
		REQUIRE(partitioner.ExtractPipelineTasks(*plan, tiny_sql, 3).size() == 1);

		const string sql = "SELECT id FROM object_db.t";
		plan = reader.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		auto row_group_info = analyzer.ExtractRowGroupInfo(*plan);
		REQUIRE(row_group_info.total_row_groups >= 3);
		auto tasks = partitioner.ExtractPipelineTasks(*plan, sql, 3);
		REQUIRE(tasks.size() == 3);
		idx_t rows = 0;
		idx_t next_row_group = 0;
		for (const auto &task : tasks) {
			// Each task owns the next contiguous run of row groups.
			REQUIRE(task.row_group_start == next_row_group);
			REQUIRE(task.row_group_end >= task.row_group_start);
			next_row_group = task.row_group_end + 1;
			auto result = reader.Query(StringUtil::Format("SELECT count(*) FROM (%s)", task.task_sql));
			REQUIRE_FALSE(result->HasError());
			rows += result->GetValue(0, 0).GetValue<idx_t>();
		}
		REQUIRE(next_row_group == row_group_info.total_row_groups);
		REQUIRE(rows == 10000);
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Optimizer sends a two-table Join aggregate through a worker fragment", "[worker_fragment]") {
	auto root = std::filesystem::temp_directory_path() /
	            StringUtil::Format("duckherder_join_fragment_%s",
	                               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	std::filesystem::create_directories(root);
	{
		distributed::StorageConfig config;
		config.set_database_uri("duckdb_objfs://join_fragment.db");
		config.mutable_local()->set_root(root.string());
		auto database_result = ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE);
		REQUIRE(database_result.ok());
		auto database = std::move(database_result).ValueOrDie();
		auto &db = database->GetInstance();
		auto &db_config = DBConfig::GetConfig(*db.instance);
		db_config.options.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
		OptimizerExtension::Register(db_config, GetWorkerFragmentExtension());
		auto writer_result = database->Connect();
		REQUIRE(writer_result.ok());
		auto writer = std::move(writer_result).ValueOrDie();
		REQUIRE_FALSE(writer
		                  ->Query("CREATE TABLE fact AS SELECT i AS id, i % 17 AS k, i % 11 AS amount "
		                          "FROM range(300000) t(i)")
		                  ->HasError());
		REQUIRE_FALSE(
		    writer->Query("CREATE TABLE dim AS SELECT i AS k, i + 1 AS multiplier FROM range(17) t(i)")->HasError());
		writer.reset();

		WorkerManager manager(db);
		auto client_result = database->Connect();
		auto executor_result = database->Connect();
		REQUIRE(client_result.ok());
		REQUIRE(executor_result.ok());
		auto client = std::move(client_result).ValueOrDie();
		auto executor_connection = std::move(executor_result).ValueOrDie();
		DistributedExecutor executor(manager, *executor_connection, config);
		auto state = make_shared_ptr<WorkerFragmentState>(executor, *executor_connection);
		client->context->registered_state->Insert(WorkerFragmentState::NAME, state);
		const string sql = "SELECT sum(f.amount * d.multiplier) AS revenue FROM fact f "
		                   "JOIN dim d ON f.k = d.k WHERE (d.k = 2 AND f.amount < 5) "
		                   "OR (d.k = 3 AND f.amount >= 5)";
		auto expected = executor_connection->Query(sql);
		REQUIRE_FALSE(expected->HasError());
		auto local_prepared = state->PrepareClientQuery(*client, sql);
		REQUIRE_FALSE(local_prepared->HasError());
		vector<Value> parameters;
		auto local_result = local_prepared->Execute(parameters, /*allow_stream_result=*/false);
		REQUIRE_FALSE(local_result->HasError());
		REQUIRE(local_result->Equals(*expected));
		REQUIRE(state->TakeExecutions().empty());

		REQUIRE(manager.StartLocalWorkers(2).ok());
		expected = executor_connection->Query(sql);
		REQUIRE_FALSE(expected->HasError());
		auto prepared = state->PrepareClientQuery(*client, sql);
		REQUIRE_FALSE(prepared->HasError());
		auto actual = prepared->Execute(parameters, /*allow_stream_result=*/false);
		REQUIRE_FALSE(actual->HasError());
		REQUIRE(actual->Equals(*expected));
		auto executions = state->TakeExecutions();
		REQUIRE(executions.size() == 1);
		REQUIRE(executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);
		REQUIRE(executions[0].num_tasks_generated == 2);

		REQUIRE_FALSE(executor_connection->Query("CREATE TABLE dim2 AS SELECT * FROM dim")->HasError());
		const string small_sql = "SELECT sum(d.multiplier) FROM dim d JOIN dim2 e ON d.k = e.k";
		auto small_expected = executor_connection->Query(small_sql);
		auto small_prepared = state->PrepareClientQuery(*client, small_sql);
		REQUIRE_FALSE(small_prepared->HasError());
		auto small_actual = small_prepared->Execute(parameters, /*allow_stream_result=*/false);
		REQUIRE_FALSE(small_actual->HasError());
		REQUIRE(small_actual->Equals(*small_expected));
		REQUIRE(state->TakeExecutions().empty());

		REQUIRE_FALSE(client->Query("CREATE TEMP TABLE temp_dim AS SELECT 2 AS k, 3 AS multiplier")->HasError());
		const string temp_sql = "SELECT sum(f.amount * d.multiplier) FROM fact f "
		                        "JOIN temp_dim d ON f.k = d.k";
		auto temp_prepared = state->PrepareClientQuery(*client, temp_sql);
		REQUIRE_FALSE(temp_prepared->HasError());
		auto temp_result = temp_prepared->Execute(parameters, /*allow_stream_result=*/false);
		REQUIRE_FALSE(temp_result->HasError());
		REQUIRE(state->TakeExecutions().empty());

		const string scan_sql = "SELECT sum(amount) FROM fact";
		auto scan_expected = executor_connection->Query(scan_sql);
		REQUIRE_FALSE(scan_expected->HasError());
		auto scan_prepared = client->Prepare(scan_sql);
		REQUIRE_FALSE(scan_prepared->HasError());
		auto scan_result = scan_prepared->Execute(parameters, /*allow_stream_result=*/false);
		REQUIRE_FALSE(scan_result->HasError());
		REQUIRE(scan_result->Equals(*scan_expected));
		auto scan_executions = state->TakeExecutions();
		REQUIRE(scan_executions.size() == 1);
		REQUIRE(scan_executions[0].execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION);

		REQUIRE_FALSE(client->Query("BEGIN TRANSACTION")->HasError());
		auto txn_prepared = state->PrepareClientQuery(*client, sql);
		REQUIRE_FALSE(txn_prepared->HasError());
		auto txn_result = txn_prepared->Execute(parameters, /*allow_stream_result=*/false);
		REQUIRE_FALSE(txn_result->HasError());
		REQUIRE(state->TakeExecutions().empty());
		REQUIRE_FALSE(client->Query("ROLLBACK")->HasError());
		client.reset();
		state.reset();
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Two-table inner join partitions one input and merges partial sums", "[task_partitioner]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE fact AS SELECT i AS id, i % 17 AS key, i % 7 AS amount, "
	                        "i % 3 = 0 AS keep FROM range(250000) t(i)")
	                  ->HasError());
	REQUIRE_FALSE(con.Query("CREATE TABLE dim AS SELECT i AS key, i + 1 AS multiplier, "
	                        "CASE WHEN i % 3 = 0 THEN 'A' ELSE 'B' END AS brand FROM range(17) t(i)")
	                  ->HasError());
	REQUIRE_FALSE(con.Query("INSERT INTO dim VALUES (3, 11, 'B'), (NULL, 9, 'A')")->HasError());
	REQUIRE_FALSE(con.Query("INSERT INTO fact VALUES (250001, NULL, 5, true), (250002, 3, NULL, true)")->HasError());
	REQUIRE_FALSE(con.Query("DELETE FROM fact WHERE id >= 10000 AND id < 20000")->HasError());
	QueryPlanAnalyzer analyzer(con);
	TaskPartitioner partitioner(con, analyzer);
	for (const auto &sql : {"SELECT sum(f.amount * d.multiplier) AS revenue FROM fact f, dim d "
	                        "WHERE f.key = d.key AND (f.keep OR d.key = 3)",
	                        "SELECT sum(f.amount * d.multiplier) AS revenue FROM fact f, dim d "
	                        "WHERE (f.key = d.key AND d.brand = 'A' AND f.keep) OR "
	                        "(f.key = d.key AND d.brand = 'B' AND f.id % 5 = 0)",
	                        "SELECT sum(f.amount * d.multiplier) AS revenue FROM fact f, dim d "
	                        "WHERE (f.key = d.key AND f.keep) OR (f.key = d.key AND d.brand = 'B' AND f.id % 5 = 0)",
	                        "SELECT d.brand, count(*), sum(f.amount), avg(f.amount) FROM fact f "
	                        "JOIN dim d ON f.key = d.key GROUP BY 1"}) {
		INFO(sql);
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		REQUIRE(IsSupportedPlan(*plan));
		auto statements = con.ExtractStatements(sql);
		auto analysis = QueryPlanAnalyzer::AnalyzeQuery(*plan, statements[0]->Cast<SelectStatement>());
		REQUIRE(analysis.supports_partitioned_aggregation);
		auto tasks = partitioner.ExtractPipelineTasks(*plan, analysis.partial_sql, 3);
		REQUIRE(tasks.size() == 3);
		REQUIRE_FALSE(con.Query(StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT * FROM (%s) WHERE false",
		                                           QueryPlanAnalyzer::PARTIAL_TABLE_NAME, analysis.partial_sql))
		                  ->HasError());
		for (const auto &task : tasks) {
			REQUIRE_FALSE(
			    con.Query(StringUtil::Format("INSERT INTO %s %s", QueryPlanAnalyzer::PARTIAL_TABLE_NAME, task.task_sql))
			        ->HasError());
		}
		auto expected = con.Query(StringUtil::Format("SELECT * FROM (%s) ORDER BY ALL", sql));
		auto actual = con.Query(StringUtil::Format("SELECT * FROM (%s) ORDER BY ALL", analysis.final_sql));
		REQUIRE_FALSE(expected->HasError());
		REQUIRE_FALSE(actual->HasError());
		REQUIRE(actual->RowCount() == expected->RowCount());
		for (idx_t row = 0; row < expected->RowCount(); ++row) {
			for (idx_t col = 0; col < expected->ColumnCount(); ++col) {
				REQUIRE(Value::NotDistinctFrom(actual->GetValue(col, row).DefaultCastAs(expected->types[col]),
				                               expected->GetValue(col, row)));
			}
		}
	}

	REQUIRE_FALSE(con.Query("CREATE TEMP TABLE temp_dim AS SELECT * FROM dim")->HasError());
	const string temp_sql = "SELECT sum(f.amount) FROM fact f JOIN temp_dim d ON f.key = d.key";
	auto temp_plan = con.ExtractPlan(temp_sql);
	REQUIRE(temp_plan != nullptr);
	REQUIRE(partitioner.ExtractPipelineTasks(*temp_plan, temp_sql, 3).size() == 1);

	const string outer_sql = "SELECT sum(f.amount) FROM fact f LEFT JOIN dim d ON f.key = d.key";
	auto plan = con.ExtractPlan(outer_sql);
	REQUIRE(plan != nullptr);
	REQUIRE_FALSE(IsSupportedPlan(*plan));
	for (const auto &sql : {"SELECT sum(f.amount) FROM fact f JOIN dim d ON f.key < d.key",
	                        "SELECT sum(f.amount) FROM fact f CROSS JOIN dim d",
	                        "SELECT sum(j.amount) FROM (fact f JOIN dim d ON f.key = d.key) AS j"}) {
		plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		REQUIRE(partitioner.ExtractPipelineTasks(*plan, sql, 3).size() == 1);
	}
}

TEST_CASE("Partial aggregate SQL preserves global aggregate semantics", "[partial_aggregate]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE aggregates(category VARCHAR, value INTEGER, keep BOOLEAN)")->HasError());
	REQUIRE_FALSE(con.Query("INSERT INTO aggregates VALUES ('a', 1, true), ('a', NULL, true), ('a', 9, true), "
	                        "('b', -2, true), ('b', 20, true), ('c', 100, false)")
	                  ->HasError());

	// Queries in the form pushed down by remote clients.
	for (const auto &sql : {"SELECT sum(value), count(*), count(value), min(value), max(value), avg(value) "
	                        "FROM aggregates",
	                        "SELECT category, count(*), sum(value), min(value), max(value), avg(value) "
	                        "FROM aggregates WHERE keep GROUP BY 1",
	                        "SELECT \"%\"(value, CAST(2 AS INTEGER)), sum(\"*\"(value, CAST(3 AS INTEGER))), "
	                        "max(\"length\"(category)) FROM aggregates GROUP BY 1",
	                        "SELECT category FROM aggregates GROUP BY 1",
	                        "SELECT sum(value), count(*), avg(value) FROM aggregates WHERE value > 1000"}) {
		INFO(sql);
		auto analysis = AnalyzeQuery(con, sql);
		REQUIRE(analysis.supports_partitioned_aggregation);

		// Compute partial aggregates on two partitions, then merge them.
		REQUIRE_FALSE(con.Query(StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT * FROM (%s) WHERE false",
		                                           QueryPlanAnalyzer::PARTIAL_TABLE_NAME, analysis.partial_sql))
		                  ->HasError());
		for (const auto &predicate : {"rowid < 3", "rowid >= 3"}) {
			auto task_sql = PartitionSQLGenerator::InjectWhereClause(analysis.partial_sql, predicate);
			REQUIRE_FALSE(
			    con.Query(StringUtil::Format("INSERT INTO %s %s", QueryPlanAnalyzer::PARTIAL_TABLE_NAME, task_sql))
			        ->HasError());
		}
		auto expected = con.Query(StringUtil::Format("SELECT * FROM (%s) ORDER BY ALL", sql));
		auto actual = con.Query(StringUtil::Format("SELECT * FROM (%s) ORDER BY ALL", analysis.final_sql));
		REQUIRE_FALSE(expected->HasError());
		REQUIRE_FALSE(actual->HasError());
		REQUIRE(actual->RowCount() == expected->RowCount());
		for (idx_t row = 0; row < expected->RowCount(); ++row) {
			for (idx_t col = 0; col < expected->ColumnCount(); ++col) {
				REQUIRE(Value::NotDistinctFrom(actual->GetValue(col, row).DefaultCastAs(expected->types[col]),
				                               expected->GetValue(col, row)));
			}
		}
	}
}

TEST_CASE("Unsupported aggregates retain fallback", "[partial_aggregate]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE aggregates(category INTEGER, value INTEGER, span INTERVAL)")->HasError());
	for (const auto &sql :
	     {"SELECT median(value) FROM aggregates", "SELECT sum(DISTINCT value) FROM aggregates",
	      "SELECT sum(value) FILTER (WHERE value > 0) FROM aggregates", "SELECT avg(span) FROM aggregates",
	      "SELECT category, sum(value) FROM aggregates GROUP BY category",
	      "SELECT category, sum(value) FROM aggregates GROUP BY 1 HAVING sum(value) > 0",
	      "SELECT category, sum(value) FROM aggregates GROUP BY ROLLUP (1)", "SELECT sum(value) + 1 FROM aggregates"}) {
		INFO(sql);
		REQUIRE_FALSE(AnalyzeQuery(con, sql).supports_partitioned_aggregation);
	}
}

TEST_CASE("Supported plans are decided by plan operators, not SQL keywords", "[query_utils]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE t(id INTEGER, note VARCHAR)")->HasError());
	REQUIRE_FALSE(con.Query("INSERT INTO t SELECT range, range::VARCHAR FROM range(10)")->HasError());

	for (const auto &sql :
	     {"SELECT id FROM t WHERE id > 1", "SELECT note, count(*) FROM t GROUP BY note",
	      "SELECT id FROM t JOIN t t2 USING (id)", "SELECT id FROM t WHERE note <> 'x ORDER BY y OFFSET 1'"}) {
		INFO(sql);
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		REQUIRE(IsSupportedPlan(*plan));
	}
	for (const auto &sql : {"SELECT id FROM t ORDER BY id", "SELECT id FROM t\nORDER\nBY id",
	                        "SELECT id FROM t LIMIT 1", "SELECT id FROM t LIMIT 1 OFFSET 1", "SELECT 1"}) {
		INFO(sql);
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		REQUIRE_FALSE(IsSupportedPlan(*plan));
	}
}
