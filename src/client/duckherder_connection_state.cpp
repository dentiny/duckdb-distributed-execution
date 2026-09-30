#include "client/duckherder_connection_state.hpp"

#include "client/execution/distributed_client.hpp"

namespace duckdb {

DuckherderConnectionState::DuckherderConnectionState(string server_url, distributed::ClientRole role,
                                                     DatabaseInstance &db_instance,
                                                     distributed::StorageConfig storage_config)
    : client(make_uniq<DistributedClient>(std::move(server_url), role, db_instance, std::move(storage_config))) {
}

DuckherderConnectionState::DuckherderConnectionState(unique_ptr<DistributedClient> client_p)
    : client(std::move(client_p)) {
}

DuckherderConnectionState::~DuckherderConnectionState() = default;

DistributedClient &DuckherderConnectionState::GetClient() {
	return *client;
}

void DuckherderConnectionState::Close() {
	client->Close();
}

} // namespace duckdb
