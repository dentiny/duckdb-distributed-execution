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
}

TEST_CASE("Reject invalid Duckherder remote endpoints", "[duckherder][endpoint]") {
	const char *invalid_endpoints[] {
	    ":memory:",    "local.duckdb",    "http://localhost:8815", "localhost", "localhost:",
	    "localhost:0", "localhost:65536", "localhost:abc",         "::1:8815",  "grpc://localhost",
	};

	for (const auto &endpoint : invalid_endpoints) {
		DYNAMIC_SECTION(endpoint) {
			REQUIRE_THROWS_AS(ParseRemoteEndpoint(endpoint), InvalidInputException);
		}
	}
}
