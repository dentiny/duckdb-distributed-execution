#pragma once

#include "client.pb.h"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "storage.pb.h"

namespace duckdb {

class DatabaseInstance;
class DistributedClient;

// Owns the remote session associated with one DuckDB ClientContext and one
// Duckherder catalog. Its lifetime is tied to ClientContext::registered_state.
class DuckherderConnectionState : public ClientContextState {
public:
	DuckherderConnectionState(string server_url, distributed::ClientRole role, DatabaseInstance &db_instance,
	                          distributed::StorageConfig storage_config);
	explicit DuckherderConnectionState(unique_ptr<DistributedClient> client);
	~DuckherderConnectionState() override;

	DistributedClient &GetClient();
	void Close();

private:
	unique_ptr<DistributedClient> client;
};

} // namespace duckdb
