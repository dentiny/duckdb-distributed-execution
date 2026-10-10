#include "catch/catch.hpp"

#include "arrow_utils.hpp"
#include "distributed.pb.h"
#include "flight_test_utils.hpp"
#include "server/driver/worker_manager.hpp"
#include "server/object_storage_database.hpp"
#include "server/validation.hpp"
#include "server/worker/worker_node.hpp"

#include <arrow/util/future.h>
#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT

TEST_CASE("Test Flight server startup and connection", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	auto status = client.Connect();

	REQUIRE(status.ok());
}

TEST_CASE("Expired writer lease can be reclaimed", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	struct LeaseTimeoutReset {
		explicit LeaseTimeoutReset(DistributedFlightServer &server_p) : server(server_p) {
		}
		~LeaseTimeoutReset() {
			server.GetTestStateForTesting().SetClientLeaseTimeout(std::chrono::seconds(30));
		}
		DistributedFlightServer &server;
	} timeout_reset(server);

	server.GetTestStateForTesting().SetClientLeaseTimeout(std::chrono::milliseconds(1));
	DistributedFlightClient expired_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(expired_writer.Connect().ok());
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	DistributedFlightClient replacement_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(replacement_writer.Connect().ok());
}

TEST_CASE("Server reset clears writer admission", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient old_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(old_writer.Connect().ok());

	server.Reset();

	DistributedFlightClient replacement_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(replacement_writer.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(old_writer.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE_FALSE(response.success());
}

TEST_CASE("Autocommit operations avoid transaction lifecycle RPCs", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	auto transaction_request_count = server.GetTestStateForTesting().GetTransactionRequestCount();
	ExecuteAutocommit(client, "CREATE TABLE autocommit_single_rpc (id INTEGER)");
	ExecuteAutocommit(client, "INSERT INTO autocommit_single_rpc VALUES (1)");
	REQUIRE(CountRows(client, "autocommit_single_rpc") == 1);
	REQUIRE(server.GetTestStateForTesting().GetTransactionRequestCount() == transaction_request_count);
}

TEST_CASE("Each client owns an isolated DuckDB connection", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	DistributedFlightClient reader(SERVER_URL, distributed::CLIENT_ROLE_READ_ONLY);
	REQUIRE(writer.Connect().ok());
	REQUIRE(reader.Connect().ok());

	distributed::DistributedResponse response;
	ExecuteAutocommit(writer, "CREATE TABLE client_connection_isolation (id INTEGER)");
	ExecuteAutocommit(writer, "INSERT INTO client_connection_isolation VALUES (1)");

	REQUIRE(writer.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(writer.ExecuteStatement("INSERT INTO client_connection_isolation VALUES (2)", "", response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(reader, "client_connection_isolation") == 1);

	// Closing the writer destroys its server-side connection and rolls back the open transaction.
	writer.Close();
	DistributedFlightClient replacement_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(replacement_writer.Connect().ok());
	REQUIRE(CountRows(replacement_writer, "client_connection_isolation") == 1);

	ExecuteAutocommit(replacement_writer, "INSERT INTO client_connection_isolation VALUES (3)");
	REQUIRE(CountRows(reader, "client_connection_isolation") == 2);
}

TEST_CASE("Read-only clients cannot write through scans", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	DistributedFlightClient reader(SERVER_URL, distributed::CLIENT_ROLE_READ_ONLY);
	REQUIRE(writer.Connect().ok());
	REQUIRE(reader.Connect().ok());
	ExecuteAutocommit(writer, "CREATE TABLE reader_scan_write (id INTEGER)");

	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	auto status = reader.ScanTable("INSERT INTO reader_scan_write SELECT 1", 100, 0, batches);
	REQUIRE_FALSE(status.ok());
	REQUIRE_THAT(status.ToString(), Catch::Contains("read-only"));
	REQUIRE(CountRows(reader, "reader_scan_write") == 0);
}

TEST_CASE("Test TableExists via protobuf", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse create_resp;
	auto status = client.ExecuteStatement("CREATE TABLE test_exists (id INTEGER)", "", create_resp);
	REQUIRE(status.ok());
	REQUIRE(create_resp.success());

	bool exists = false;
	status = client.TableExists("test_exists", exists);
	REQUIRE(status.ok());
	REQUIRE(exists);

	bool not_exists = false;
	status = client.TableExists("nonexistent_table", not_exists);
	REQUIRE(status.ok());
	REQUIRE_FALSE(not_exists);
}

TEST_CASE("Test error handling in protobuf responses", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	auto status = client.ExecuteStatement("INVALID SQL SYNTAX", "", response);

	REQUIRE(status.ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(response.has_error());
	REQUIRE(response.error().exception_type() == distributed::REMOTE_EXCEPTION_PARSER);
	REQUIRE_FALSE(response.error().message().empty());
}

TEST_CASE("S3 storage identity excludes credentials", "[distributed_flight][object_storage]") {
	distributed::StorageConfig first;
	first.set_database_uri("duckdb_objfs://database.db");
	auto first_s3 = first.mutable_s3();
	first_s3->set_bucket("bucket");
	first_s3->set_root("prefix");
	first_s3->set_endpoint("localhost:9000");
	first_s3->set_key_id("first-key");
	first_s3->set_secret("first-secret");
	first_s3->set_use_ssl(false);
	first_s3->set_url_style(distributed::S3_URL_STYLE_PATH);
	REQUIRE(ValidateRequest(first).ok());

	auto second = first;
	second.mutable_s3()->set_key_id("second-key");
	second.mutable_s3()->set_secret("second-secret");
	REQUIRE(ObjectStorageDatabase::GetKey(first) == ObjectStorageDatabase::GetKey(second));

	second.mutable_s3()->set_endpoint("other-endpoint:9000");
	REQUIRE(ObjectStorageDatabase::GetKey(first) != ObjectStorageDatabase::GetKey(second));

	auto invalid = first;
	invalid.mutable_s3()->clear_secret();
	REQUIRE_FALSE(ValidateRequest(invalid).ok());
}

TEST_CASE("Worker dispatch pool limits concurrency and reuses threads", "[dispatch_pool]") {
	DuckDB db(nullptr);
	WorkerManager manager(db);
	auto result = manager.GetDispatchPool(2);
	ARROW_THROW_IF_ERROR(result);
	auto pool = *result;
	auto gate = arrow::Future<>::Make();
	auto two_started = arrow::Future<>::Make();
	std::atomic<idx_t> started {0};
	vector<arrow::Future<>> futures;
	futures.reserve(3);
	for (idx_t idx = 0; idx < 3; ++idx) {
		auto submitted = pool->Submit([&]() {
			if (++started == 2) {
				two_started.MarkFinished();
			}
			gate.Wait();
			return arrow::Status::OK();
		});
		if (!submitted.ok()) {
			gate.MarkFinished();
			for (auto &future : futures) {
				future.Wait();
			}
			FAIL(submitted.status().ToString());
		}
		futures.push_back(*submitted);
	}
	const auto reached_two = two_started.Wait(5.0);
	const auto running_before_release = started.load();
	gate.MarkFinished();
	for (auto &future : futures) {
		future.Wait();
	}
	for (auto &future : futures) {
		ARROW_THROW_IF_ERROR(future.status());
	}
	REQUIRE(reached_two);
	REQUIRE(running_before_release == 2);
	REQUIRE(started.load() == 3);
	auto resized = manager.GetDispatchPool(1);
	ARROW_THROW_IF_ERROR(resized);
	REQUIRE(*resized == pool);
	REQUIRE(pool->GetCapacity() == 1);
}

TEST_CASE("DUCKHERDER_STARTUP_SQL runs in every database the server and workers create", "[distributed_flight]") {
	auto config = ObjectStorageDatabase::ResolveConfig(distributed::StorageConfig {});
	// Unset the variable even when a REQUIRE fails, so later tests don't run this SQL.
	struct StartupSQLReset {
		~StartupSQLReset() {
			unsetenv("DUCKHERDER_STARTUP_SQL");
		}
	} startup_sql_reset;

	setenv("DUCKHERDER_STARTUP_SQL", "SET GLOBAL threads = 3; SET GLOBAL threads = 5", /*overwrite=*/1);
	auto database = ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE);
	REQUIRE(database.ok());
	Connection conn((*database)->GetInstance());
	REQUIRE(conn.Query("SELECT current_setting('threads')")->GetValue(0, 0) == Value::BIGINT(5));
	// The constructor only creates the driver's database; Start() would bind the port.
	DistributedFlightServer server("localhost", 18900);
	Connection server_conn(server.GetDatabaseInstance());
	REQUIRE(server_conn.Query("SELECT current_setting('threads')")->GetValue(0, 0) == Value::BIGINT(5));

	// The failing statement follows one that returns a result, so its error must not be lost in the result chain.
	setenv("DUCKHERDER_STARTUP_SQL", "SELECT 1; SELECT * FROM missing_table", /*overwrite=*/1);
	REQUIRE_FALSE(ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE).ok());
	REQUIRE_THROWS(WorkerNode("startup-sql-worker", "localhost", 18899));
	REQUIRE_THROWS(DistributedFlightServer("localhost", 18900));
}
