#include "client/duckherder_remote_endpoint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <charconv>
#include <utility>
#include <string_view>

namespace duckdb {

RemoteEndpoint ParseRemoteEndpoint(const string &path) {
	constexpr std::string_view GRPC_SCHEME = "grpc://";
	std::string_view endpoint(path);
	if (endpoint.substr(0, GRPC_SCHEME.size()) == GRPC_SCHEME) {
		endpoint.remove_prefix(GRPC_SCHEME.size());
	} else if (endpoint.find("://") != string::npos) {
		throw InvalidInputException("Duckherder ATTACH only supports grpc:// endpoints");
	}

	string database_name;
	const auto database_separator = endpoint.find('/');
	if (database_separator != string::npos) {
		database_name = endpoint.substr(database_separator + 1);
		if (database_name.empty() || database_name.find('/') != string::npos) {
			throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected 'host:port/database_name'", path);
		}
		endpoint = endpoint.substr(0, database_separator);
	}

	idx_t port_separator = 0;
	if (!endpoint.empty() && endpoint[0] == '[') {
		const auto closing_bracket = endpoint.find(']');
		if (closing_bracket == string::npos || closing_bracket + 1 >= endpoint.size() ||
		    endpoint[closing_bracket + 1] != ':') {
			throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected '[ipv6-address]:port'", path);
		}
		port_separator = closing_bracket + 1;
	} else {
		port_separator = endpoint.rfind(':');
		if (port_separator == string::npos || endpoint.find(':') != port_separator) {
			throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected 'host:port'", path);
		}
	}

	auto host = string(endpoint.substr(0, port_separator));
	const auto port_text = endpoint.substr(port_separator + 1);
	int port = 0;
	const auto parse_result = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
	if (host.empty() || port_text.empty() || parse_result.ec != std::errc() ||
	    parse_result.ptr != port_text.data() + port_text.size() || port < 1 || port > 65535) {
		throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected 'host:port' with a valid port", path);
	}
	return {std::move(host), port, std::move(database_name)};
}

} // namespace duckdb
