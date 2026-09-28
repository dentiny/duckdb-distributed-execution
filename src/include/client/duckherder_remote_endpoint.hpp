#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct RemoteEndpoint {
	string host;
	int port;
};

RemoteEndpoint ParseRemoteEndpoint(const string &path);

} // namespace duckdb
