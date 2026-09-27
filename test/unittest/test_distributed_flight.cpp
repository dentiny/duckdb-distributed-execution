#include "catch/catch.hpp"

#include "client/distributed_flight_client.hpp"
#include "distributed.pb.h"
#include "server/driver/distributed_flight_server.hpp"

#include <chrono>
#include <iostream>
#include <thread>

using namespace duckdb; // NOLINT

namespace {

const std::string SERVER_HOST = "0.0.0.0";
const int SERVER_PORT = 18815;
const std::string SERVER_URL = "grpc://localhost:18815";

class FlightTestServer {
public:
	FlightTestServer() : server(std::make_unique<DistributedFlightServer>(SERVER_HOST, SERVER_PORT)) {
		auto status = server->Start();
		if (!status.ok()) {
			throw std::runtime_error("Failed to start server: " + status.ToString());
		}
		server_thread = std::thread([this] {
			auto serve_status = server->Serve();
			if (!serve_status.ok()) {
				std::cerr << "Server error: " << serve_status.ToString() << std::endl;
			}
		});
		std::this_thread::sleep_for(std::chrono::seconds(2));
	}

	~FlightTestServer() {
		server->Shutdown();
		if (server_thread.joinable()) {
			server_thread.join();
		}
	}

	DistributedFlightServer &GetServer() {
		return *server;
	}

private:
	std::unique_ptr<DistributedFlightServer> server;
	std::thread server_thread;
};

FlightTestServer &GetTestServer() {
	static FlightTestServer test_server;
	return test_server;
}

} // namespace

TEST_CASE("Test Flight server startup and connection", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL);
	auto status = client.Connect();

	REQUIRE(status.ok());
}

TEST_CASE("Expired writer lease can be reclaimed", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	struct LeaseTimeoutReset {
		explicit LeaseTimeoutReset(DistributedFlightServer &server_p) : server(server_p) {
		}
		~LeaseTimeoutReset() {
			server.SetClientLeaseTimeoutForTesting(std::chrono::seconds(30));
		}
		DistributedFlightServer &server;
	} timeout_reset(server);

	server.SetClientLeaseTimeoutForTesting(std::chrono::milliseconds(1));
	DistributedFlightClient expired_writer(SERVER_URL);
	REQUIRE(expired_writer.Connect().ok());
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	DistributedFlightClient replacement_writer(SERVER_URL);
	REQUIRE(replacement_writer.Connect().ok());
}

TEST_CASE("Server reset clears writer admission", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient old_writer(SERVER_URL);
	REQUIRE(old_writer.Connect().ok());

	server.Reset();

	DistributedFlightClient replacement_writer(SERVER_URL);
	REQUIRE(replacement_writer.Connect().ok());

	bool exists = false;
	REQUIRE_FALSE(old_writer.TableExists("reset_invalidates_old_client", exists).ok());
}

TEST_CASE("Test TableExists via protobuf", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse create_resp;
	auto status = client.CreateTable("CREATE TABLE test_exists (id INTEGER)", create_resp);
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
	DistributedFlightClient client(SERVER_URL);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	auto status = client.ExecuteSQL("INVALID SQL SYNTAX", response);

	REQUIRE(status.ok());
	REQUIRE_FALSE(response.success());
	REQUIRE_FALSE(response.error_message().empty());
}
