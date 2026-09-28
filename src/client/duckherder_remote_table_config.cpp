#include "client/duckherder_remote_table_config.hpp"

#include <utility>

namespace duckdb {

RemoteTableConfig::RemoteTableConfig(string server_url_p, string remote_table_name_p)
    : server_url(std::move(server_url_p)), remote_table_name(std::move(remote_table_name_p)) {
}

} // namespace duckdb
