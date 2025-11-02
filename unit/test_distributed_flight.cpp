#define CATCH_CONFIG_RUNNER

#include "catch.hpp"
#include "distributed_flight_server.hpp"
#include "distributed_flight_client.hpp"
#include "distributed_protocol.hpp"

#include <thread>
#include <chrono>
#include <iostream>

using namespace duckdb; // NOLINT

namespace {

// Global server instance
std::unique_ptr<DistributedFlightServer> g_server;
std::thread g_server_thread;
const std::string SERVER_HOST = "0.0.0.0";
const int SERVER_PORT = 18815; // Use different port to avoid conflicts
const std::string SERVER_URL = "grpc://localhost:18815";

void StartServerInBackground() {
	g_server = std::make_unique<DistributedFlightServer>(SERVER_HOST, SERVER_PORT);

	auto status = g_server->Start();
	if (!status.ok()) {
		throw std::runtime_error("Failed to start server: " + status.ToString());
	}
	std::cout << "Test server started on " << SERVER_URL << std::endl;

	// Run server (blocking)
	auto serve_status = g_server->Serve();
	if (!serve_status.ok()) {
		std::cerr << "Server error: " << serve_status.ToString() << std::endl;
	}
}

void SetupTestServer() {
	// Start server in background thread.
	g_server_thread = std::thread(StartServerInBackground);

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
	std::cout << "Test server shut down" << std::endl;
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

int main(int argc, char **argv) {
	std::cout << "Setting up test server..." << std::endl;
	SetupTestServer();

	int result = Catch::Session().run(argc, argv);

	std::cout << "Tearing down test server..." << std::endl;
	TeardownTestServer();

	return result;
}
