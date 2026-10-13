#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/planner.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/driver/worker_node_client.hpp"
#include "server/object_storage_database.hpp"
#include "server/worker/worker_node.hpp"
#include "utils/network_utils.hpp"

#include <arrow/array.h>
#include <chrono>
#include <filesystem>

using namespace duckdb; // NOLINT

namespace {

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

	distributed::StorageConfig config;
	unique_ptr<ObjectStorageDatabase> database;
	unique_ptr<WorkerManager> manager;
	unique_ptr<Connection> client;
	unique_ptr<Connection> executor_connection;
	unique_ptr<DistributedExecutor> executor;
	shared_ptr<WorkerFragmentState> state;
};

} // namespace

TEST_CASE("Workers reject plans of another build and run their own", "[worker_fragment]") {
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
	}
	std::filesystem::remove_all(root);
}
