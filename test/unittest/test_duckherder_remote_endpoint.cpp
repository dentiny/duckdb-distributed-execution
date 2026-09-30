#include "catch/catch.hpp"

#include "client/duckherder_remote_endpoint.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb; // NOLINT

namespace {

struct ValidEndpoint {
	const char *path;
	const char *host;
	int port;
	const char *database_name;
};

struct InvalidEndpoint {
	const char *path;
	const char *error;
};

} // namespace

TEST_CASE("Parse Duckherder remote endpoints", "[duckherder][endpoint]") {
	const ValidEndpoint endpoints[] {
	    {"localhost:8815", "localhost", 8815, ""},
	    {"grpc://duckherder.example.com:443", "duckherder.example.com", 443, ""},
	    {"123.45.67.89:45", "123.45.67.89", 45, ""},
	    {"[::1]:8815", "[::1]", 8815, ""},
	    {"grpc://[2001:db8::1]:8815", "[2001:db8::1]", 8815, ""},
	    {"localhost:1", "localhost", 1, ""},
	    {"localhost:65535", "localhost", 65535, ""},
	    {"localhost:8815/db_name", "localhost", 8815, "db_name"},
	    {"grpc://123.45.67.89:45/db_name", "123.45.67.89", 45, "db_name"},
	    {"[::1]:8815/shared.db", "[::1]", 8815, "shared.db"},
	    {"grpc://[2001:db8::1]:8815/shared.db", "[2001:db8::1]", 8815, "shared.db"},
	};

	for (const auto &expected : endpoints) {
		DYNAMIC_SECTION(expected.path) {
			auto endpoint = ParseRemoteEndpoint(expected.path);
			REQUIRE(endpoint.host == expected.host);
			REQUIRE(endpoint.port == expected.port);
			REQUIRE(endpoint.database_name == expected.database_name);
		}
	}
}

TEST_CASE("Reject invalid Duckherder remote endpoints", "[duckherder][endpoint]") {
	const InvalidEndpoint endpoints[] {
	    {"http://localhost:8815", "only supports grpc:// endpoints"},
	    {"s3://bucket/db_name", "only supports grpc:// endpoints"},
	    {"localhost:8815/", "expected 'host:port/database_name'"},
	    {"localhost:8815/a/b", "expected 'host:port/database_name'"},
	    {"[::1]", "expected '[ipv6-address]:port'"},
	    {"[::1]8815", "expected '[ipv6-address]:port'"},
	    {"[::1:8815", "expected '[ipv6-address]:port'"},
	    {":memory:", "expected 'host:port'"},
	    {"local.duckdb", "expected 'host:port'"},
	    {"localhost", "expected 'host:port'"},
	    {"grpc://localhost", "expected 'host:port'"},
	    {"localhost/db_name", "expected 'host:port'"},
	    {"::1:8815", "expected 'host:port'"},
	    {"", "expected 'host:port'"},
	    {":8815", "with a valid port"},
	    {"localhost:", "with a valid port"},
	    {"localhost:0", "with a valid port"},
	    {"localhost:65536", "with a valid port"},
	    {"localhost:-1", "with a valid port"},
	    {"localhost:abc", "with a valid port"},
	    {"localhost:8815abc", "with a valid port"},
	    {"[::1]:", "with a valid port"},
	};

	for (const auto &invalid : endpoints) {
		DYNAMIC_SECTION(invalid.path) {
			REQUIRE_THROWS_AS(ParseRemoteEndpoint(invalid.path), InvalidInputException);
			REQUIRE_THROWS_WITH(ParseRemoteEndpoint(invalid.path), Catch::Contains(invalid.error));
		}
	}
}
