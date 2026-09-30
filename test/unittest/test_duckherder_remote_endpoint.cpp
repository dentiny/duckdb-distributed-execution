#include "catch/catch.hpp"

#include "client/duckherder_remote_endpoint.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Parse Duckherder remote endpoints", "[duckherder][endpoint]") {
	SECTION("Hostname and port") {
		auto endpoint = ParseRemoteEndpoint("localhost:8815");
		REQUIRE(endpoint.host == "localhost");
		REQUIRE(endpoint.port == 8815);
	}

	SECTION("Explicit gRPC scheme") {
		auto endpoint = ParseRemoteEndpoint("grpc://duckherder.example.com:443");
		REQUIRE(endpoint.host == "duckherder.example.com");
		REQUIRE(endpoint.port == 443);
	}

	SECTION("Bracketed IPv6 address") {
		auto endpoint = ParseRemoteEndpoint("[::1]:8815");
		REQUIRE(endpoint.host == "[::1]");
		REQUIRE(endpoint.port == 8815);
	}

	SECTION("Without database name") {
		REQUIRE(ParseRemoteEndpoint("localhost:8815").database_name.empty());
	}

	SECTION("Database name") {
		auto endpoint = ParseRemoteEndpoint("grpc://123.45.67.89:45/db_name");
		REQUIRE(endpoint.host == "123.45.67.89");
		REQUIRE(endpoint.port == 45);
		REQUIRE(endpoint.database_name == "db_name");
	}

	SECTION("IPv6 address with database name") {
		auto endpoint = ParseRemoteEndpoint("[::1]:8815/shared.db");
		REQUIRE(endpoint.host == "[::1]");
		REQUIRE(endpoint.port == 8815);
		REQUIRE(endpoint.database_name == "shared.db");
	}
}

TEST_CASE("Reject invalid Duckherder remote endpoints", "[duckherder][endpoint]") {
	const char *invalid_endpoints[] {
	    ":memory:",        "local.duckdb",       "http://localhost:8815", "localhost", "localhost:",
	    "localhost:0",     "localhost:65536",    "localhost:abc",         "::1:8815",  "grpc://localhost",
	    "localhost:8815/", "localhost:8815/a/b", "localhost/db_name",
	};

	for (const auto &endpoint : invalid_endpoints) {
		DYNAMIC_SECTION(endpoint) {
			REQUIRE_THROWS_AS(ParseRemoteEndpoint(endpoint), InvalidInputException);
		}
	}
}
