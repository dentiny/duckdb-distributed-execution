#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct RemoteEndpoint {
	string host;
	int port;
	// Object storage database selected by 'host:port/database_name'; empty selects the default in-memory database.
	string database_name;
};

RemoteEndpoint ParseRemoteEndpoint(const string &path);

} // namespace duckdb
