#include "catch/catch.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"

#include <arrow/api.h>
#include <chrono>
#include <filesystem>

using namespace duckdb; // NOLINT

namespace {

class DispatchTestWorker : public arrow::flight::FlightServerBase {
public:
	~DispatchTestWorker() override {
		(void)Shutdown();
	}

	arrow::Status DoGet(const arrow::flight::ServerCallContext &, const arrow::flight::Ticket &ticket,
	                    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
		distributed::DistributedRequest request;
		if (!request.ParseFromString(ticket.ticket) || !request.has_execute_partition()) {
			return arrow::Status::Invalid("Expected a partition request");
		}
		const auto task_id = request.execute_partition().partition_id();
		partition_ids.push_back(task_id);
		if (fail) {
			return arrow::Status::IOError("Injected task failure");
		}
		arrow::Int64Builder builder;
		ARROW_RETURN_NOT_OK(builder.Append(task_id));
		ARROW_ASSIGN_OR_RAISE(auto values, builder.Finish());
		auto schema = arrow::schema({arrow::field("id", arrow::int64())});
		auto batch = arrow::RecordBatch::Make(schema, 1, {values});
		ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}, schema));
		*stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
		return arrow::Status::OK();
	}

	bool fail = false;
	vector<uint64_t> partition_ids;
};

} // namespace

TEST_CASE("Worker dispatch preserves task assignment and failure semantics", "[distributed_executor][dispatch_pool]") {
	struct TemporaryRoot {
		std::filesystem::path path =
		    std::filesystem::temp_directory_path() /
		    ("duckherder_dispatch_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
		~TemporaryRoot() {
			std::filesystem::remove_all(path);
		}
	} root;
	std::filesystem::create_directories(root.path);
	distributed::StorageConfig config;
	config.set_database_uri("duckdb_objfs://dispatch.db");
	config.mutable_local()->set_root(root.path.string());
	auto database_result = ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE);
	REQUIRE(database_result.ok());
	auto database = std::move(database_result).ValueOrDie();
	auto connection_result = database->Connect();
	REQUIRE(connection_result.ok());
	auto connection = std::move(connection_result).ValueOrDie();
	REQUIRE_FALSE(connection->Query("CREATE TABLE items AS SELECT i AS id FROM range(150000) t(i)")->HasError());
	REQUIRE_FALSE(connection->Query("SET GLOBAL duckherder_async_threads = 1")->HasError());

	DispatchTestWorker first, second, unused;
	WorkerManager manager(database->GetInstance());
	idx_t worker_idx = 0;
	for (auto *worker : {&first, &second, &unused}) {
		auto location = arrow::flight::Location::ForGrpcTcp("localhost", 0);
		REQUIRE(location.ok());
		REQUIRE(worker->Init(arrow::flight::FlightServerOptions(*location)).ok());
		REQUIRE(
		    manager.RegisterWorker(std::to_string(worker_idx++), "grpc://localhost:" + std::to_string(worker->port()))
		        .ok());
	}
	DistributedExecutor executor(manager, *connection, config);

	SECTION("Queued tasks map directly to workers and retain their result order") {
		auto result = executor.ExecuteDistributed("SELECT id FROM items", DistributedFragmentKind::TABLE);
		REQUIRE(result.num_tasks == 2);
		REQUIRE(result.num_workers_used == 2);
		REQUIRE(result.arrow_batches.size() == 2);
		for (idx_t task_idx = 0; task_idx < result.arrow_batches.size(); ++task_idx) {
			REQUIRE(result.arrow_batches[task_idx]->num_rows() == 1);
			auto &ids = static_cast<const arrow::Int64Array &>(*result.arrow_batches[task_idx]->column(0));
			REQUIRE(ids.Value(0) == task_idx);
		}
	}
	SECTION("A failed first task waits for the queued task before falling back") {
		first.fail = true;
		auto result = executor.ExecuteDistributed("SELECT id FROM items", DistributedFragmentKind::TABLE);
		REQUIRE(result.result == nullptr);
		REQUIRE(result.arrow_batches.empty());
	}
	SECTION("A failed last task does not merge the earlier successful task") {
		second.fail = true;
		auto result = executor.ExecuteDistributed("SELECT id FROM items", DistributedFragmentKind::TABLE);
		REQUIRE(result.result == nullptr);
		REQUIRE(result.arrow_batches.empty());
	}
	SECTION("Submission failure remains a dispatch exception") {
		auto pool_result = manager.GetDispatchPool(1);
		REQUIRE(pool_result.ok());
		REQUIRE((*pool_result)->Shutdown().ok());
		REQUIRE_THROWS_WITH(executor.ExecuteDistributed("SELECT id FROM items", DistributedFragmentKind::TABLE),
		                    Catch::Contains("Worker dispatch failed"));
		REQUIRE(first.partition_ids.empty());
		REQUIRE(second.partition_ids.empty());
		return;
	}
	REQUIRE(first.partition_ids == vector<uint64_t> {0});
	REQUIRE(second.partition_ids == vector<uint64_t> {1});
	REQUIRE(unused.partition_ids.empty());
}
