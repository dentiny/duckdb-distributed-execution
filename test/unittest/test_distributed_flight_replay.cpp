#include "catch/catch.hpp"

#include "distributed.pb.h"
#include "flight_test_utils.hpp"

using namespace duckdb; // NOLINT

// Retries of transaction lifecycle requests and operations whose responses were lost or ambiguous.

TEST_CASE("Lost COMMIT response is recovered idempotently", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	ExecuteAutocommit(client, "CREATE TABLE lost_commit_response (id INTEGER)");
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO lost_commit_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::COMMIT_RESPONSE);
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
	ExecuteAutocommit(client, "CREATE TABLE persistent_commit_response_loss (id INTEGER)");
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO persistent_commit_response_loss VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::COMMIT_RESPONSE, 100);
	REQUIRE_FALSE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::COMMIT_RESPONSE, 0);
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
	ExecuteAutocommit(client, "CREATE TABLE delivered_unknown_response (id INTEGER)");

	distributed::DistributedResponse response;
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::UNKNOWN_TRANSACTION_RESPONSE);
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN);
	// This BEGIN reconciles and rolls back the previous ambiguous BEGIN before allocating a new TID.
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO delivered_unknown_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::UNKNOWN_TRANSACTION_RESPONSE);
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

TEST_CASE("ROLLBACK resolves an UNKNOWN BEGIN outcome", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::UNKNOWN_TRANSACTION_RESPONSE);
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN);

	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_ROLLED_BACK);

	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
}

TEST_CASE("An operation resolves an UNKNOWN COMMIT outcome", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAutocommit(client, "CREATE TABLE operation_after_unknown_commit (id INTEGER)");

	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ExecuteStatement("INSERT INTO operation_after_unknown_commit VALUES (1)", "", response).ok());
	REQUIRE(response.success());

	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::UNKNOWN_TRANSACTION_RESPONSE);
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE_FALSE(response.success());
	REQUIRE(response.transaction().status() == distributed::TRANSACTION_STATUS_UNKNOWN);

	ExecuteAutocommit(client, "INSERT INTO operation_after_unknown_commit VALUES (2)");
	REQUIRE_FALSE(client.HasActiveTransaction());

	DistributedFlightClient reader(SERVER_URL, distributed::CLIENT_ROLE_READ_ONLY);
	REQUIRE(reader.Connect().ok());
	REQUIRE(CountRows(reader, "operation_after_unknown_commit") == 2);
}

TEST_CASE("Lost DML response replays one operation within its transaction", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAutocommit(client, "CREATE TABLE lost_dml_response (id INTEGER)");

	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::EXECUTE_STATEMENT_RESPONSE);
	REQUIRE(client.ExecuteStatement("INSERT INTO lost_dml_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());
	// Identical SQL with a new request sequence is a distinct operation and must still execute.
	REQUIRE(client.ExecuteStatement("INSERT INTO lost_dml_response VALUES (1)", "", response).ok());
	REQUIRE(response.success());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(client, "lost_dml_response") == 2);
}

TEST_CASE("Lost autocommit DML response replays without lifecycle RPCs", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAutocommit(client, "CREATE TABLE lost_autocommit_response (id INTEGER)");

	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::EXECUTE_STATEMENT_RESPONSE);
	ExecuteAutocommit(client, "INSERT INTO lost_autocommit_response VALUES (1)");
	REQUIRE(CountRows(client, "lost_autocommit_response") == 1);
}

TEST_CASE("Exhausted DML response loss requires transaction rollback", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAutocommit(client, "CREATE TABLE exhausted_dml_response (id INTEGER)");

	distributed::DistributedResponse response;
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_BEGIN, response).ok());
	REQUIRE(response.success());
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::EXECUTE_STATEMENT_RESPONSE, 100);
	REQUIRE_FALSE(client.ExecuteStatement("INSERT INTO exhausted_dml_response VALUES (1)", "", response).ok());
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::EXECUTE_STATEMENT_RESPONSE, 0);
	REQUIRE_FALSE(client.ManageTransaction(distributed::TRANSACTION_ACTION_COMMIT, response).ok());
	REQUIRE(client.ManageTransaction(distributed::TRANSACTION_ACTION_ROLLBACK, response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(client, "exhausted_dml_response") == 0);
}

TEST_CASE("Lost query response replays the materialized result", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());
	ExecuteAutocommit(client, "CREATE TABLE lost_query_response (id INTEGER)");
	ExecuteAutocommit(client, "INSERT INTO lost_query_response VALUES (1)");

	auto query_count = server.GetQueryExecutions().size();
	server.GetTestStateForTesting().InjectFault(DistributedFlightServerTestFault::SCAN_RESPONSE);
	REQUIRE(CountRows(client, "lost_query_response") == 1);
	REQUIRE(server.GetQueryExecutions().size() == query_count + 1);
}
