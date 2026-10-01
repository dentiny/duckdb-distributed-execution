#include "catch/catch.hpp"

#include "duckdb/common/exception.hpp"
#include "utils/remote_error.hpp"

using namespace duckdb;

namespace {

ErrorData RoundTrip(ExceptionType type, const string &message) {
	distributed::RemoteError remote_error;
	ToRemoteError(ErrorData(type, message), remote_error);
	return FromRemoteError(remote_error);
}

} // namespace

TEST_CASE("Remote errors keep their exception type", "[remote_error]") {
	auto error = RoundTrip(ExceptionType::CONVERSION, "bad cast");
	REQUIRE(error.Type() == ExceptionType::CONVERSION);
	REQUIRE(error.RawMessage() == "bad cast");
}

TEST_CASE("Remote errors that invalidate a database become IO errors", "[remote_error]") {
	auto internal_error = RoundTrip(ExceptionType::INTERNAL, "boom");
	REQUIRE(internal_error.Type() == ExceptionType::IO);
	REQUIRE(internal_error.RawMessage() == "Remote INTERNAL Error: boom");

	auto fatal_error = RoundTrip(ExceptionType::FATAL, "database has been invalidated");
	REQUIRE(fatal_error.Type() == ExceptionType::IO);
	REQUIRE(fatal_error.RawMessage() == "Remote FATAL Error: database has been invalidated");
}
