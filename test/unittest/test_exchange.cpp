#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner.hpp"
#include "server/driver/plan_serializer.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "server/driver/worker_node_client.hpp"
#include "server/object_storage_database.hpp"
#include "server/worker/exchange_read.hpp"
#include "server/worker/worker_node.hpp"
#include "utils/network_utils.hpp"

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <chrono>
#include <filesystem>

using namespace duckdb; // NOLINT

namespace {

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

// Returns the slot holding the plan's only scan.
unique_ptr<LogicalOperator> &FindScan(unique_ptr<LogicalOperator> &plan) {
	auto *op = &plan;
	while ((*op)->children.size() == 1) {
		op = &(*op)->children[0];
	}
	REQUIRE((*op)->type == LogicalOperatorType::LOGICAL_GET);
	return *op;
}

std::vector<std::shared_ptr<arrow::RecordBatch>>
ReadAll(arrow::Result<std::unique_ptr<arrow::flight::FlightStreamReader>> stream) {
	INFO((stream.ok() ? "" : stream.status().ToString()));
	REQUIRE(stream.ok());
	auto batches = (*stream)->ToRecordBatches();
	INFO((batches.ok() ? "" : batches.status().ToString()));
	REQUIRE(batches.ok());
	return *batches;
}

struct GroupResult {
	int64_t count;
	int64_t sum;

	bool operator==(const GroupResult &other) const {
		return count == other.count && sum == other.sum;
	}
};

int64_t Int64At(const std::shared_ptr<arrow::RecordBatch> &batch, int column, int64_t row) {
	return std::static_pointer_cast<arrow::Int64Array>(batch->column(column))->Value(row);
}

// A served database holding t, whose k is not stored in order, so every row group holds keys of every bucket.
struct ExchangeFixture {
	explicit ExchangeFixture(const string &name)
	    : root(std::filesystem::temp_directory_path() /
	           StringUtil::Format("%s_%s", name,
	                              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {
		std::filesystem::create_directories(root);
		config.set_database_uri("duckdb_objfs://exchange.db");
		config.mutable_local()->set_root(root.string());
		auto database_result = ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE);
		REQUIRE(database_result.ok());
		database = std::move(database_result).ValueOrDie();
		auto con_result = database->Connect();
		REQUIRE(con_result.ok());
		con = std::move(con_result).ValueOrDie();
		REQUIRE_FALSE(
		    con->Query("CREATE TABLE t AS SELECT i AS id, (i * 7919) % 1000 AS k FROM range(300000) t(i)")->HasError());
	}
	~ExchangeFixture() {
		con.reset();
		database.reset();
		std::filesystem::remove_all(root);
	}

	// Returns a producer task of exchange_id running sql, which must end with the bucket column, over all of t.
	distributed::ExecutePartitionRequest Producer(const string &sql, const string &exchange_id, idx_t bucket_count) {
		auto plan = OptimizePlan(*con, sql);
		distributed::ExecutePartitionRequest request;
		request.set_serialized_plan(PlanSerializer::SerializeLogicalPlan(*plan));
		*request.mutable_storage_config() = config;
		request.set_build_id(PlanSerializer::BuildId());
		request.mutable_exchange_write()->set_exchange_id(exchange_id);
		request.mutable_exchange_write()->set_bucket_count(NumericCast<uint32_t>(bucket_count));
		return request;
	}

	std::filesystem::path root;
	distributed::StorageConfig config;
	unique_ptr<ObjectStorageDatabase> database;
	unique_ptr<Connection> con;
};

} // namespace

TEST_CASE("Exchanges move rows between workers by bucket", "[exchange]") {
	ExchangeFixture fixture("duckherder_exchange");
	{
		auto &config = fixture.config;
		auto &con = *fixture.con;
		vector<unique_ptr<WorkerNode>> workers;
		vector<unique_ptr<WorkerNodeClient>> clients;
		vector<string> locations;
		for (idx_t idx = 0; idx < 2; ++idx) {
			workers.push_back(make_uniq<WorkerNode>(StringUtil::Format("exchange_worker_%llu", idx), "localhost",
			                                        GetAvailablePort(9600 + 50 * NumericCast<int>(idx))));
			REQUIRE(workers.back()->Start().ok());
			locations.push_back(workers.back()->GetLocation());
			clients.push_back(make_uniq<WorkerNodeClient>(locations.back()));
			REQUIRE(clients.back()->Connect().ok());
		}

		// Producers: each worker reads its row groups of t and buckets rows by k < 500.
		auto producer = OptimizePlan(con, "SELECT id, k, (CASE WHEN k < 500 THEN 0 ELSE 1 END)::INTEGER FROM t");
		auto &producer_scan = FindScan(producer)->Cast<LogicalGet>();
		vector<string> producer_plans;
		con.context->RunFunctionInTransaction(
		    [&]() { producer_plans = SerializeTaskPlans(*con.context, *producer, producer_scan, 2); });
		REQUIRE(producer_plans.size() == 2);
		vector<int64_t> bucket_rows(2, 0);
		for (idx_t task = 0; task < 2; ++task) {
			distributed::ExecutePartitionRequest request;
			request.set_serialized_plan(producer_plans[task]);
			*request.mutable_storage_config() = config;
			request.set_build_id(PlanSerializer::BuildId());
			request.mutable_exchange_write()->set_exchange_id("x");
			request.mutable_exchange_write()->set_bucket_count(2);
			auto batches = ReadAll(clients[task]->ExecutePartition(request));
			REQUIRE(batches.size() == 1);
			REQUIRE(batches[0]->num_rows() == 2);
			for (idx_t bucket = 0; bucket < 2; ++bucket) {
				auto &bucket_counts = static_cast<const arrow::UInt64Array &>(*batches[0]->column(0));
				auto rows = bucket_counts.Value(NumericCast<int64_t>(bucket));
				// Both tasks see keys of both buckets.
				REQUIRE(rows > 0);
				bucket_rows[bucket] += NumericCast<int64_t>(rows);
			}
		}
		REQUIRE(bucket_rows[0] == 150000);
		REQUIRE(bucket_rows[1] == 150000);

		// Consumers: the scan of t is replaced by the bucket of each worker, so each group is complete on one worker.
		// Maps from k to its group's count and sum.
		map<int64_t, GroupResult> groups;
		for (idx_t bucket = 0; bucket < 2; ++bucket) {
			auto consumer = OptimizePlan(con, "SELECT k, count(*)::BIGINT, sum(id)::BIGINT FROM t GROUP BY k");
			auto &slot = FindScan(consumer);
			auto &scan = slot->Cast<LogicalGet>();
			constexpr idx_t EXCHANGE_TABLE_INDEX = 1000;
			ColumnBindingReplacer replacer;
			auto bindings = scan.GetColumnBindings();
			auto &column_ids = scan.GetColumnIds();
			for (idx_t idx = 0; idx < bindings.size(); ++idx) {
				auto column = column_ids[scan.projection_ids.empty() ? idx : scan.projection_ids[idx]];
				// The producer's columns are id, then k, as in t.
				replacer.replacement_bindings.emplace_back(
				    bindings[idx],
				    ColumnBinding(TableIndex(EXCHANGE_TABLE_INDEX), ProjectionIndex(column.GetPrimaryIndex())));
			}
			vector<LogicalType> types {LogicalType {LogicalTypeId::BIGINT}, LogicalType {LogicalTypeId::BIGINT}};
			slot =
			    MakeExchangeRead(EXCHANGE_TABLE_INDEX, /*exchange=*/0, bucket, std::move(types), {"id", "k"}, 150000);
			replacer.VisitOperator(*consumer);

			distributed::ExecutePartitionRequest request;
			request.set_serialized_plan(PlanSerializer::SerializeLogicalPlan(*consumer));
			*request.mutable_storage_config() = config;
			request.set_build_id(PlanSerializer::BuildId());
			auto &source = *request.add_exchange_sources();
			source.set_exchange_id("x");
			for (const auto &location : locations) {
				source.add_producers(location);
			}
			// A bucket can be read more than once, as a plan scanning it twice does.
			for (idx_t read = 0; read < 2; ++read) {
				auto batches = ReadAll(clients[bucket]->ExecutePartition(request));
				for (auto &batch : batches) {
					for (int64_t row_idx = 0; row_idx < batch->num_rows(); ++row_idx) {
						auto k = Int64At(batch, 0, row_idx);
						REQUIRE((k < 500) == (bucket == 0));
						GroupResult value {Int64At(batch, 1, row_idx), Int64At(batch, 2, row_idx)};
						auto entry = groups.emplace(k, value);
						// The second read returns the same groups.
						REQUIRE((entry.second || entry.first->second == value));
					}
				}
			}
		}
		auto expected = con.Query("SELECT k, count(*)::BIGINT, sum(id)::BIGINT FROM t GROUP BY k ORDER BY k");
		REQUIRE_FALSE(expected->HasError());
		REQUIRE(groups.size() == expected->RowCount());
		auto &expected_rows = expected->Collection();
		for (idx_t row_idx = 0; row_idx < expected->RowCount(); ++row_idx) {
			auto &group = groups[expected_rows.GetValue(0, row_idx).GetValue<int64_t>()];
			REQUIRE(group.count == expected_rows.GetValue(1, row_idx).GetValue<int64_t>());
			REQUIRE(group.sum == expected_rows.GetValue(2, row_idx).GetValue<int64_t>());
		}

		// Released exchanges cannot be read.
		for (auto &client : clients) {
			REQUIRE(client->ReleaseExchange("x").ok());
		}
		auto stream = clients[0]->ReadExchange("x", 0);
		REQUIRE_FALSE(stream.ok());
		REQUIRE(StringUtil::Contains(stream.status().ToString(), "has no exchange x"));

		for (auto &worker : workers) {
			worker->Shutdown();
		}
	}
}

TEST_CASE("Exchanges are readable only once their producer finishes", "[exchange]") {
	ExchangeFixture fixture("duckherder_exchange_producer");
	WorkerNode worker("exchange_producer_worker", "localhost", GetAvailablePort(9700));
	REQUIRE(worker.Start().ok());
	WorkerNodeClient client(worker.GetLocation());
	REQUIRE(client.Connect().ok());

	// A producer that fails leaves its partial rows unreadable.
	auto failing = fixture.Producer("SELECT id, (CASE WHEN id < 200000 THEN 0 ELSE 5 END)::INTEGER FROM t", "bad", 2);
	auto stream = client.ExecutePartition(failing);
	auto status = stream.ok() ? (*stream)->ToRecordBatches().status() : stream.status();
	REQUIRE(StringUtil::Contains(status.ToString(), "Exchange row has no valid bucket"));
	stream = client.ReadExchange("bad", 0);
	REQUIRE_FALSE(stream.ok());
	REQUIRE(StringUtil::Contains(stream.status().ToString(), "still being written or its producer failed"));

	// A second producer of the same exchange on a worker would add its rows twice.
	auto producer = fixture.Producer("SELECT id, (k % 2)::INTEGER FROM t", "x", 2);
	REQUIRE(ReadAll(client.ExecutePartition(producer)).size() == 1);
	stream = client.ExecutePartition(producer);
	REQUIRE_FALSE(stream.ok());
	REQUIRE(StringUtil::Contains(stream.status().ToString(), "already has a producer"));
	REQUIRE(ReadAll(client.ReadExchange("x", 1)).size() == 2);
	worker.Shutdown();
}
