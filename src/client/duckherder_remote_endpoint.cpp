#include "client/duckherder_remote_endpoint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <charconv>
#include <utility>

namespace duckdb {

RemoteEndpoint ParseRemoteEndpoint(const string &path) {
	// TODO(hjiang): use string_view after DuckDB V2.0 release.
	constexpr const char *GRPC_SCHEME = "grpc://";
	constexpr idx_t GRPC_SCHEME_LENGTH = 7;
	auto endpoint = path;
	if (StringUtil::StartsWith(endpoint, GRPC_SCHEME)) {
		endpoint = endpoint.substr(GRPC_SCHEME_LENGTH);
	} else if (endpoint.find("://") != string::npos) {
		throw InvalidInputException("Duckherder ATTACH only supports grpc:// endpoints");
	}

	idx_t port_separator;
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

	auto host = endpoint.substr(0, port_separator);
	const auto port_text = endpoint.substr(port_separator + 1);
	int port = 0;
	const auto parse_result = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
	if (host.empty() || port_text.empty() || parse_result.ec != std::errc() ||
	    parse_result.ptr != port_text.data() + port_text.size() || port < 1 || port > 65535) {
		throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected 'host:port' with a valid port", path);
	}
	return {std::move(host), port};
}

} // namespace duckdb
