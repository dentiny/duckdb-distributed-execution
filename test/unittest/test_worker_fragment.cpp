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
		OptimizerExtension::Register(DBConfig::GetConfig(*database->GetInstance().instance),
		                             GetWorkerFragmentExtension());
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
		const string aggregates = "sum(n), sum(d15), min(s), max(d38), min(g), count(maybe), count(*)";
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
		// Aggregates without a merge, such as avg or the top-N max(x, n), are computed on the driver from a scan
		// fragment.
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
		auto executions = fixture.RequireLocalResult("SELECT k, sum(id) FROM t GROUP BY k ORDER BY k");
		REQUIRE(executions.size() == 1);
		REQUIRE(executions[0].execution_mode == QueryExecutionMode::LOCAL);
		REQUIRE(StringUtil::StartsWith(executions[0].sql, "aggregate "));
	}
	std::filesystem::remove_all(root);
}
