#include "client/duckherder_client_sessions.hpp"

#include "client/duckherder_connection_state.hpp"
#include "client/execution/distributed_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

DuckherderClientSessions::DuckherderClientSessions(string server_url_p, distributed::ClientRole role_p,
                                                   DatabaseInstance &db_instance_p,
                                                   distributed::StorageConfig storage_config_p,
                                                   connection_t attach_connection_id_p)
    : server_url(std::move(server_url_p)), role(role_p), db_instance(db_instance_p),
      storage_config(std::move(storage_config_p)),
      state_key(StringUtil::Format("duckherder_client_%s", UUID::ToString(UUID::GenerateRandomUUID()))),
      attach_connection_id(attach_connection_id_p) {
	attach_client = make_uniq<DistributedClient>(server_url, role, db_instance, storage_config);
}

DuckherderClientSessions::~DuckherderClientSessions() = default;

DistributedClient &DuckherderClientSessions::GetClient(ClientContext &context) {
	concurrency::lock_guard<concurrency::mutex> lock(mu);
	if (detached) {
		throw InvalidInputException("Duckherder attachment is detached");
	}
	if (role == distributed::CLIENT_ROLE_READ_WRITE) {
		EnsureWriteOwner(context);
	}
	return GetOrCreateState(context)->GetClient();
}

void DuckherderClientSessions::Close() {
	vector<shared_ptr<DuckherderConnectionState>> live_states;
	unique_ptr<DistributedClient> pending_client;
	{
		concurrency::lock_guard<concurrency::mutex> lock(mu);
		detached = true;
		pending_client = std::move(attach_client);
		for (auto &entry : states) {
			auto state = entry.second.lock();
			if (state) {
				live_states.emplace_back(std::move(state));
			}
		}
		states.clear();
	}
	if (pending_client) {
		pending_client->Close();
	}
	for (auto &state : live_states) {
		state->Close();
	}
}

void DuckherderClientSessions::RemoveState(ClientContext &context) {
	context.registered_state->Remove(state_key);
}

void DuckherderClientSessions::EnsureWriteOwner(ClientContext &context) {
	if (context.GetConnectionId() == attach_connection_id) {
		return;
	}
	auto owner_state = states.find(attach_connection_id);
	if (owner_state != states.end() && !owner_state->second.expired()) {
		throw InvalidInputException(
		    "A read-write Duckherder attachment can only be used by the DuckDB connection that attached it; "
		    "attach a separate Duckherder database with READ_ONLY access from this connection");
	}
	attach_connection_id = context.GetConnectionId();
}

shared_ptr<DuckherderConnectionState> DuckherderClientSessions::GetOrCreateState(ClientContext &context) {
	shared_ptr<DuckherderConnectionState> state;
	if (context.GetConnectionId() == attach_connection_id && attach_client) {
		state = context.registered_state->GetOrCreate<DuckherderConnectionState>(state_key, std::move(attach_client));
	} else {
		state = context.registered_state->GetOrCreate<DuckherderConnectionState>(state_key, server_url, role,
		                                                                         db_instance, storage_config);
	}
	PruneExpiredStates();
	states[context.GetConnectionId()] = state;
	return state;
}

void DuckherderClientSessions::PruneExpiredStates() {
	for (auto entry = states.begin(); entry != states.end();) {
		if (entry->second.expired()) {
			entry = states.erase(entry);
		} else {
			++entry;
		}
	}
}

} // namespace duckdb
