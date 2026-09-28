#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct RemoteTableConfig {
	string server_url;
	string remote_table_name;

	RemoteTableConfig(string server_url_p, string remote_table_name_p);
};

// Maps from a case-insensitive logical table name in the main schema to its remote routing configuration.
using RemoteTableMap = case_insensitive_map_t<RemoteTableConfig>;

} // namespace duckdb
