#include "catch/catch.hpp"

#include "client/transport/distributed_flight_client.hpp"
#include "distributed.pb.h"
#include "server/driver/distributed_flight_server.hpp"
#include "utils/no_destructor.hpp"

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
		// Arrow Flight can finish Serve() before its gRPC worker cleanup is visible to the server destructor.
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		server.reset();
	}

	DistributedFlightServer &GetServer() {
		return *server;
	}

private:
	std::unique_ptr<DistributedFlightServer> server;
	std::thread server_thread;
};

FlightTestServer &GetTestServer() {
	// Arrow/gRPC teardown can race its worker cleanup on macOS; let process exit release this test fixture.
	static NoDestructor<FlightTestServer> test_server;
	return *test_server;
}

// Scan all batches returned by the server and count their rows.
uint64_t CountRows(DistributedFlightClient &client, const string &table_name) {
	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	REQUIRE(client.ScanTable(table_name, 100, 0, batches).ok());
	uint64_t row_count = 0;
	for (const auto &batch : batches) {
		row_count += batch->num_rows();
	}
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(response.success());
	return row_count;
}

void ExecuteAndCommit(DistributedFlightClient &client, const string &sql) {
	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement(sql, "", response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(response.success());
}

} // namespace

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
			server.SetClientLeaseTimeoutForTesting(std::chrono::seconds(30));
		}
		DistributedFlightServer &server;
	} timeout_reset(server);

	server.SetClientLeaseTimeoutForTesting(std::chrono::milliseconds(1));
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

TEST_CASE("Each client owns an isolated DuckDB connection", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	DistributedFlightClient reader(SERVER_URL, distributed::CLIENT_ROLE_READ_ONLY);
	REQUIRE(writer.Connect().ok());
	REQUIRE(reader.Connect().ok());

	distributed::DistributedResponse response;
	ExecuteAndCommit(writer, "CREATE TABLE client_connection_isolation (id INTEGER)");
	ExecuteAndCommit(writer, "INSERT INTO client_connection_isolation VALUES (1)");

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

	ExecuteAndCommit(replacement_writer, "INSERT INTO client_connection_isolation VALUES (3)");
	REQUIRE(CountRows(reader, "client_connection_isolation") == 2);
}

TEST_CASE("Lost COMMIT response is recovered idempotently", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	ExecuteAndCommit(client, "CREATE TABLE lost_commit_response (id INTEGER)");
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO lost_commit_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.FailNextCommitResponseForTesting();
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_COMMITTED);
	REQUIRE(CountRows(client, "lost_commit_response") == 1);
}

TEST_CASE("Persistent COMMIT response loss has a recoverable unknown outcome", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	ExecuteAndCommit(client, "CREATE TABLE persistent_commit_response_loss (id INTEGER)");
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO persistent_commit_response_loss VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.FailCommitResponsesForTesting(100);
	REQUIRE_FALSE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	server.FailCommitResponsesForTesting(0);
	DistributedFlightClient reader(SERVER_URL, distributed::CLIENT_ROLE_READ_ONLY);
	REQUIRE(reader.Connect().ok());
	REQUIRE(CountRows(reader, "persistent_commit_response_loss") == 1);

	// The next BEGIN first reconciles the retained COMMIT identifier, then starts a new transaction.
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_ACTIVE);
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(reader, "persistent_commit_response_loss") == 1);
}

TEST_CASE("Delivered UNKNOWN transaction responses are reconciled", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAndCommit(client, "CREATE TABLE delivered_unknown_response (id INTEGER)");

	distributed::DistributedResponse response;
	server.ReturnUnknownTransactionResponsesForTesting(1);
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN);
	// This BEGIN reconciles and rolls back the previous ambiguous BEGIN before allocating a new TID.
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO delivered_unknown_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.ReturnUnknownTransactionResponsesForTesting(1);
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN);
	// This BEGIN first replays the ambiguous COMMIT, then starts the next transaction.
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(client, "delivered_unknown_response") == 1);
}

TEST_CASE("Lost DML response replays one operation within its transaction", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAndCommit(client, "CREATE TABLE lost_dml_response (id INTEGER)");

	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	server.FailNextExecuteStatementResponseForTesting();
	REQUIRE(client.ExecuteStatement("INSERT INTO lost_dml_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());
	// Identical SQL with a new request sequence is a distinct operation and must still execute.
	REQUIRE(client.ExecuteStatement("INSERT INTO lost_dml_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(client, "lost_dml_response") == 2);
}

TEST_CASE("Exhausted DML response loss requires transaction rollback", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAndCommit(client, "CREATE TABLE exhausted_dml_response (id INTEGER)");

	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	server.FailExecuteStatementResponsesForTesting(100);
	REQUIRE_FALSE(client.ExecuteStatement("INSERT INTO exhausted_dml_response VALUES (1)", "", response).ok());
	server.FailExecuteStatementResponsesForTesting(0);
	REQUIRE_FALSE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(client, "exhausted_dml_response") == 0);
}

TEST_CASE("Lost query response replays the materialized result", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAndCommit(client, "CREATE TABLE lost_query_response (id INTEGER)");
	ExecuteAndCommit(client, "INSERT INTO lost_query_response VALUES (1)");

	auto query_count = server.GetQueryExecutions().size();
	server.FailNextScanResponseForTesting();
	REQUIRE(CountRows(client, "lost_query_response") == 1);
	REQUIRE(server.GetQueryExecutions().size() == query_count + 1);
}

TEST_CASE("Test TableExists via protobuf", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse create_resp;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, create_resp).ok());
	REQUIRE(create_resp.success());
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
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, create_resp).ok());
	REQUIRE(create_resp.success());
}

TEST_CASE("Test error handling in protobuf responses", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	auto status = client.ExecuteStatement("INVALID SQL SYNTAX", "", response);

	REQUIRE(status.ok());
	REQUIRE_FALSE(response.success());
	REQUIRE_FALSE(response.error_message().empty());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
}
