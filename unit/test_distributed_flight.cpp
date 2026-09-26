#define CATCH_CONFIG_RUNNER

#include "catch.hpp"
#include "client/distributed_client.hpp"
#include "client/distributed_flight_client.hpp"
#include "client/duckherder_transaction_manager.hpp"
#include "distributed.pb.h"
#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "server/driver/distributed_flight_server.hpp"

#include <chrono>
#include <iostream>
#include <thread>

using namespace duckdb; // NOLINT

namespace {

// Global server instance
std::unique_ptr<DistributedFlightServer> g_server;
std::thread g_server_thread;
const std::string SERVER_HOST = "0.0.0.0";
const int SERVER_PORT = 18815; // Use different port to avoid conflicts
const std::string SERVER_URL = "grpc://localhost:18815";

void ServeInBackground() {
	// Run server (blocking)
	auto serve_status = g_server->Serve();
	if (!serve_status.ok()) {
		std::cerr << "Server error: " << serve_status.ToString() << std::endl;
	}
}

void SetupTestServer() {
	g_server = std::make_unique<DistributedFlightServer>(SERVER_HOST, SERVER_PORT);
	auto status = g_server->Start();
	if (!status.ok()) {
		throw std::runtime_error("Failed to start server: " + status.ToString());
	}
	std::cout << "Test server started on " << SERVER_URL << std::endl;

	// Start server in background thread.
	g_server_thread = std::thread(ServeInBackground);

	// Wait for server to be ready.
	std::this_thread::sleep_for(std::chrono::seconds(2));
}

void TeardownTestServer() {
	if (g_server) {
		g_server->Shutdown();
	}
	if (g_server_thread.joinable()) {
		g_server_thread.join();
	}
	g_server.reset();
	std::cout << "Test server shut down" << std::endl;
}

struct TestServerGuard {
	TestServerGuard() {
		SetupTestServer();
	}
	~TestServerGuard() {
		TeardownTestServer();
	}
};

uint64_t ScanRowCount(DistributedFlightClient &client, const string &table_name, const string &session_id = "",
                      idx_t limit = 100, idx_t offset = 0) {
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	ScanTableOptions options;
	options.session_id = session_id;
	REQUIRE(client.ScanTable(table_name, limit, offset, stream, options).ok());
	uint64_t row_count = 0;
	while (true) {
		auto next = stream->Next();
		REQUIRE(next.ok());
		auto batch = next.ValueOrDie().data;
		if (!batch) {
			break;
		}
		row_count += batch->num_rows();
	}
	return row_count;
}

} // namespace

TEST_CASE("Test Flight server startup and connection", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	auto status = client.Connect();

	REQUIRE(status.ok());
}

TEST_CASE("Test CreateTable via protobuf", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	// Send CreateTableRequest (protobuf with oneof)
	distributed::DistributedResponse response;
	auto status = client.CreateTable("CREATE TABLE test_users (id INTEGER, name VARCHAR)", response);

	REQUIRE(status.ok());
	REQUIRE(response.success());
	REQUIRE(response.has_create_table());
}

TEST_CASE("Test TableExists via protobuf", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	// Create table first.
	distributed::DistributedResponse create_resp;
	auto status = client.CreateTable("CREATE TABLE test_exists (id INTEGER)", create_resp);
	REQUIRE(status.ok());
	REQUIRE(create_resp.success());

	// Check existence via TableExistsRequest.
	bool exists = false;
	status = client.TableExists("test_exists", exists);
	REQUIRE(status.ok());
	REQUIRE(exists == true);

	// Check non-existent table.
	bool not_exists = false;
	status = client.TableExists("nonexistent_table", not_exists);
	REQUIRE(status.ok());
	REQUIRE(not_exists == false);
}

TEST_CASE("Test Insert and Scan via protobuf returning Arrow", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	// Create table.
	distributed::DistributedResponse create_resp;
	auto status = client.CreateTable("CREATE TABLE test_scan (id INTEGER, name VARCHAR, age INTEGER)", create_resp);
	REQUIRE(status.ok());
	REQUIRE(create_resp.success());

	// Insert data via ExecuteSQLRequest.
	distributed::DistributedResponse insert_resp;
	status = client.ExecuteSQL("INSERT INTO test_scan VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
	                           insert_resp);
	REQUIRE(status.ok());
	REQUIRE(insert_resp.success());
	REQUIRE(insert_resp.execute_sql().rows_affected() == 3);

	// Scan via ScanTableRequest.
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	status = client.ScanTable("test_scan", 100, 0, stream);
	REQUIRE(status.ok());

	// Check Arrow RecordBatches.
	uint64_t total_rows = 0;
	int batch_count = 0;

	while (true) {
		auto result = stream->Next();
		REQUIRE(result.ok());

		auto batch_with_metadata = result.ValueOrDie();
		if (!batch_with_metadata.data) {
			break;
		}

		auto batch = batch_with_metadata.data;
		batch_count++;
		total_rows += batch->num_rows();

		REQUIRE(batch->num_columns() == 3);
		REQUIRE(batch->num_rows() > 0);
	}

	REQUIRE(batch_count >= 1);
	REQUIRE(total_rows == 3);

	ScanTableOptions projection;
	projection.project_columns = true;
	projection.projected_columns = {"name"};
	status = client.ScanTable("test_scan", 100, 0, stream, projection);
	REQUIRE(status.ok());
	auto projected_batch = stream->Next();
	REQUIRE(projected_batch.ok());
	REQUIRE(projected_batch.ValueOrDie().data->num_columns() == 1);
	REQUIRE(projected_batch.ValueOrDie().data->schema()->field(0)->name() == "name");

	projection.include_rowid = true;
	status = client.ScanTable("test_scan", 100, 0, stream, projection);
	REQUIRE(status.ok());
	auto rowid_batch = stream->Next();
	REQUIRE(rowid_batch.ok());
	REQUIRE(rowid_batch.ValueOrDie().data->num_columns() == 2);
	REQUIRE(rowid_batch.ValueOrDie().data->schema()->field(0)->name() == "rowid");
}

TEST_CASE("ScanTable treats table names as identifiers", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	auto status = client.ScanTable("test_scan; DROP TABLE test_scan", 100, 0, stream);
	REQUIRE_FALSE(status.ok());

	bool exists = false;
	REQUIRE(client.TableExists("test_scan", exists).ok());
	REQUIRE(exists);
}

TEST_CASE("ScanTable requires exact Arrow schema width", "[distributed_flight]") {
	DistributedClient client(SERVER_URL);

	vector<LogicalType> too_few {LogicalType::INTEGER, LogicalType::VARCHAR};
	auto extra_columns = client.ScanTable("test_scan", 100, 0, &too_few);
	REQUIRE(extra_columns->HasError());

	vector<LogicalType> too_many {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER,
	                              LogicalType::BOOLEAN};
	auto missing_columns = client.ScanTable("test_scan", 100, 0, &too_many);
	REQUIRE(missing_columns->HasError());
}

TEST_CASE("Test error handling in protobuf responses", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	// Try to create table with invalid SQL.
	distributed::DistributedResponse response;
	auto status = client.ExecuteSQL("INVALID SQL SYNTAX", response);

	REQUIRE(status.ok());                           // RPC succeeded
	REQUIRE(response.success() == false);           // But SQL failed
	REQUIRE(response.error_message().length() > 0); // Has error message
}

TEST_CASE("Test DropTable via protobuf", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	// Create table first.
	distributed::DistributedResponse create_resp;
	auto status = client.CreateTable("CREATE TABLE test_drop (id INTEGER)", create_resp);
	REQUIRE(status.ok());
	REQUIRE(create_resp.success());

	// Verify it exists.
	bool exists = false;
	status = client.TableExists("test_drop", exists);
	REQUIRE(status.ok());
	REQUIRE(exists == true);

	// Drop via DropTableRequest.
	distributed::DistributedResponse drop_resp;
	status = client.DropTable("test_drop", drop_resp);
	REQUIRE(status.ok());
	REQUIRE(drop_resp.success());
	REQUIRE(drop_resp.has_drop_table());

	// Verify it no longer exists.
	exists = true;
	status = client.TableExists("test_drop", exists);
	REQUIRE(status.ok());
	REQUIRE(exists == false);
}

TEST_CASE("Test session open and close are idempotent", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	string session_id = "client-generated-idempotent-session";
	REQUIRE(client.OpenSession(session_id).ok());
	REQUIRE(client.OpenSession(session_id).ok());

	distributed::DistributedResponse close_response;
	REQUIRE(client.CloseSession(session_id, close_response).ok());
	REQUIRE(close_response.success());

	distributed::DistributedResponse second_close_response;
	REQUIRE(client.CloseSession(session_id, second_close_response).ok());
	REQUIRE(second_close_response.success());

	distributed::DistributedResponse execute_response;
	REQUIRE(client.ExecuteSQL("SELECT 42", execute_response, session_id).ok());
	REQUIRE_FALSE(execute_response.success());
}

TEST_CASE("Test protocol version mismatch fails cleanly", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	string session_id;
	REQUIRE_FALSE(client.OpenSession(session_id, DUCKHERDER_PROTOCOL_VERSION + 1).ok());

	ScanTableOptions options;
	options.protocol_version = DUCKHERDER_PROTOCOL_VERSION + 1;
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	REQUIRE_FALSE(client.ScanTable("test_scan", 100, 0, stream, options).ok());
}

TEST_CASE("Test expired sessions roll back and are reclaimed", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE lease_rollback (id INTEGER)", response).ok());
	REQUIRE(response.success());

	string expired_session;
	REQUIRE(client.OpenSession(expired_session).ok());
	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, expired_session).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteSQL("INSERT INTO lease_rollback VALUES (1)", response, expired_session).ok());
	REQUIRE(response.success());

	g_server->SetSessionTimeoutForTesting(std::chrono::milliseconds(1));
	std::this_thread::sleep_for(std::chrono::milliseconds(150));
	g_server->SetSessionTimeoutForTesting(std::chrono::minutes(5));

	REQUIRE(client.ExecuteSQL("SELECT 42", response, expired_session).ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(ScanRowCount(client, "lease_rollback") == 0);
}

TEST_CASE("Test lost COMMIT response is recovered idempotently", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE ambiguous_commit (id INTEGER)", response).ok());
	REQUIRE(response.success());

	string session_id;
	REQUIRE(client.OpenSession(session_id).ok());
	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, session_id).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteSQL("INSERT INTO ambiguous_commit VALUES (1)", response, session_id).ok());
	REQUIRE(response.success());

	g_server->FailNextCommitResponseForTesting();
	REQUIRE(client.ExecuteSQL("COMMIT", response, session_id).ok());
	REQUIRE(response.success());
	REQUIRE(ScanRowCount(client, "ambiguous_commit") == 1);

	distributed::DistributedResponse close_response;
	REQUIRE(client.CloseSession(session_id, close_response).ok());
	REQUIRE(close_response.success());
}

TEST_CASE("Test persistent COMMIT response loss remains recoverable", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE persistent_commit (id INTEGER)", response).ok());
	REQUIRE(response.success());

	string session_id;
	REQUIRE(client.OpenSession(session_id).ok());
	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, session_id).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteSQL("INSERT INTO persistent_commit VALUES (1)", response, session_id).ok());
	REQUIRE(response.success());

	g_server->FailCommitResponsesForTesting(2);
	REQUIRE_FALSE(client.ExecuteSQL("COMMIT", response, session_id).ok());
	REQUIRE(ScanRowCount(client, "persistent_commit") == 1);

	// Stable session status answers a later recovery query without executing
	// COMMIT a second time.
	REQUIRE(client.ExecuteSQL("COMMIT", response, session_id).ok());
	REQUIRE(response.success());

	distributed::DistributedResponse close_response;
	REQUIRE(client.CloseSession(session_id, close_response).ok());
	REQUIRE(close_response.success());
}

TEST_CASE("Persistent COMMIT failure clears transaction pointer sidecar", "[distributed_flight]") {
	DuckDB db(nullptr);
	Connection connection(db);

	auto result =
	    connection.Query("ATTACH ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port 18815)");
	REQUIRE_FALSE(result->HasError());
	REQUIRE_FALSE(
	    connection.Query("PRAGMA duckherder_register_remote_table('manager_commit', 'manager_commit')")->HasError());
	REQUIRE_FALSE(connection.Query("CREATE TABLE dh.manager_commit (id INTEGER)")->HasError());

	REQUIRE_FALSE(connection.Query("BEGIN")->HasError());
	REQUIRE_FALSE(connection.Query("INSERT INTO dh.manager_commit VALUES (1)")->HasError());

	auto dh_db = DatabaseManager::Get(*connection.context).GetDatabase(*connection.context, "dh");
	REQUIRE(dh_db);
	auto &transaction_manager = dh_db->GetTransactionManager().Cast<DuckherderTransactionManager>();
	REQUIRE(transaction_manager.RemoteTransactionCountForTesting() == 1);

	g_server->FailCommitResponsesForTesting(2);
	auto commit = connection.Query("COMMIT");
	REQUIRE(commit->HasError());
	REQUIRE(transaction_manager.RemoteTransactionCountForTesting() == 0);
}

TEST_CASE("Known limitation: transaction manager resolves persistent COMMIT response loss",
          "[distributed_flight][known-limitation][.]") {
	DuckDB db(nullptr);
	Connection connection(db);

	REQUIRE_FALSE(
	    connection.Query("ATTACH ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port 18815)")
	        ->HasError());
	REQUIRE_FALSE(
	    connection.Query("PRAGMA duckherder_register_remote_table('manager_commit_recovery', "
	                     "'manager_commit_recovery')")
	        ->HasError());
	REQUIRE_FALSE(connection.Query("CREATE TABLE dh.manager_commit_recovery (id INTEGER)")->HasError());
	REQUIRE_FALSE(connection.Query("BEGIN")->HasError());
	REQUIRE_FALSE(connection.Query("INSERT INTO dh.manager_commit_recovery VALUES (1)")->HasError());

	g_server->FailCommitResponsesForTesting(2);
	// Desired behavior: a durable control-node transaction status resolves the
	// ambiguous transport outcome instead of surfacing a failed local COMMIT.
	REQUIRE_FALSE(connection.Query("COMMIT")->HasError());
}

TEST_CASE("Known limitation: legacy sessionless scans survive a rolling upgrade",
          "[distributed_flight][known-limitation][.]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());
	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE legacy_scan (id INTEGER)", response).ok());
	REQUIRE(response.success());

	ScanTableOptions options;
	options.protocol_version = 0;
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	REQUIRE(client.ScanTable("legacy_scan", 100, 0, stream, options).ok());
}

TEST_CASE("Known limitation: paged autocommit scans retain one statement snapshot",
          "[distributed_flight][known-limitation][.]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());
	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE snapshot_scan (id INTEGER)", response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteSQL("INSERT INTO snapshot_scan VALUES (1), (2), (3)", response).ok());
	REQUIRE(response.success());

	auto rows = ScanRowCount(client, "snapshot_scan", "", 2, 0);
	REQUIRE(client.ExecuteSQL("DELETE FROM snapshot_scan WHERE id = 1", response).ok());
	REQUIRE(response.success());
	rows += ScanRowCount(client, "snapshot_scan", "", 2, 2);

	// Both pages belong to one logical table scan and should observe one snapshot.
	REQUIRE(rows == 3);
}

TEST_CASE("Test closing a session rolls back its transaction", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE close_rollback (id INTEGER)", response).ok());
	REQUIRE(response.success());

	string session_id;
	REQUIRE(client.OpenSession(session_id).ok());
	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, session_id).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteSQL("INSERT INTO close_rollback VALUES (1)", response, session_id).ok());
	REQUIRE(response.success());

	distributed::DistributedResponse close_response;
	REQUIRE(client.CloseSession(session_id, close_response).ok());
	REQUIRE(close_response.success());
	REQUIRE(ScanRowCount(client, "close_rollback") == 0);
}

TEST_CASE("Test two sessions isolate uncommitted writes", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("CREATE TABLE two_sessions (id INTEGER)", response).ok());
	REQUIRE(response.success());

	string writer_session;
	string reader_session;
	REQUIRE(client.OpenSession(writer_session).ok());
	REQUIRE(client.OpenSession(reader_session).ok());
	REQUIRE(writer_session != reader_session);

	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, reader_session).ok());
	REQUIRE(response.success());
	REQUIRE(ScanRowCount(client, "two_sessions", reader_session) == 0);

	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, writer_session).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteSQL("INSERT INTO two_sessions VALUES (1)", response, writer_session).ok());
	REQUIRE(response.success());
	REQUIRE(ScanRowCount(client, "two_sessions", reader_session) == 0);

	REQUIRE(client.ExecuteSQL("COMMIT", response, writer_session).ok());
	REQUIRE(response.success());
	REQUIRE(ScanRowCount(client, "two_sessions", reader_session) == 0);
	REQUIRE(client.ExecuteSQL("COMMIT", response, reader_session).ok());
	REQUIRE(response.success());
	REQUIRE(ScanRowCount(client, "two_sessions", reader_session) == 1);

	distributed::DistributedResponse close_response;
	REQUIRE(client.CloseSession(writer_session, close_response).ok());
	REQUIRE(close_response.success());
	REQUIRE(client.CloseSession(reader_session, close_response).ok());
	REQUIRE(close_response.success());
}

TEST_CASE("Test reset is synchronized with session execution", "[distributed_flight]") {
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	string session_id;
	REQUIRE(client.OpenSession(session_id).ok());
	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteSQL("BEGIN TRANSACTION", response, session_id).ok());
	REQUIRE(response.success());

	std::thread reset_thread([] { g_server->Reset(); });
	REQUIRE(client.ExecuteSQL("SELECT 42", response, session_id).ok());
	reset_thread.join();

	// Reset either waited for this request or closed the session before it began;
	// both outcomes are safe and a subsequent open must remain usable.
	string new_session;
	REQUIRE(client.OpenSession(new_session).ok());
	REQUIRE(client.ExecuteSQL("SELECT 42", response, new_session).ok());
	REQUIRE(response.success());
	distributed::DistributedResponse close_response;
	REQUIRE(client.CloseSession(new_session, close_response).ok());
	REQUIRE(close_response.success());
}

TEST_CASE("Local worker shutdown remains idempotent across resets", "[distributed_flight]") {
	for (int cycle = 0; cycle < 3; ++cycle) {
		auto server = std::make_unique<DistributedFlightServer>("0.0.0.0", 18816);
		REQUIRE(server->StartWithWorkers(2).ok());
		REQUIRE(server->GetWorkerCount() == 2);

		for (int reset = 0; reset < 3; ++reset) {
			server->Reset();
			REQUIRE(server->GetWorkerCount() == 0);
			server->StartLocalWorkers(2);
			REQUIRE(server->GetWorkerCount() == 2);
		}

		server->Shutdown();
		server->Shutdown();
		server.reset();
	}
}

int main(int argc, char **argv) {
	std::cout << "Setting up test server..." << std::endl;
	TestServerGuard server_guard;
	auto result = Catch::Session().run(argc, argv);
	std::cout << "Tearing down test server..." << std::endl;
	return result;
}
