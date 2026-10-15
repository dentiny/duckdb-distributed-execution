#include "server/driver/client_registry.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "server/object_storage_database.hpp"
#include "server/validation.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistry::ClientRegistry(DistributedFlightServerTestState &test_state_p) : test_state(test_state_p) {
}

arrow::Status ClientRegistry::Register(const distributed::RegisterClientRequest &req, WorkerManager &worker_manager,
                                       distributed::DistributedResponse &resp) {
	auto validation = ValidateRequest(req);
	if (!validation.ok()) {
		resp.set_success(false);
		resp.set_error_message(validation.message());
		return arrow::Status::OK();
	}
	{
		// Expired clients release their admission when destroyed, which must happen before admitting this client.
		vector<shared_ptr<ClientRegistration>> expired;
		const concurrency::unique_lock<concurrency::shared_mutex> lock(mutex);
		PruneExpired(expired);
	}
	auto storage_config = ObjectStorageDatabase::ResolveConfig(req.storage_config());
	shared_ptr<ServedDatabase> database;
	const concurrency::unique_lock<concurrency::shared_mutex> lock(mutex);
	auto &entry = databases[ObjectStorageDatabase::GetKey(storage_config)];
	database = entry.lock();
	if (!database) {
		database = make_shared_ptr<ServedDatabase>(storage_config);
		entry = database;
		if (database->IsDefault()) {
			default_database = database;
		}
	}

	auto registration = database->AddClient(req.role(), worker_manager);
	if (!registration.ok()) {
		resp.set_success(false);
		resp.set_error_message(registration.status().message());
		return arrow::Status::OK();
	}
	auto client_id = UUID::ToString(UUID::GenerateRandomUUID());
	clients.emplace(client_id, std::move(registration).ValueOrDie());
	resp.set_success(true);
	resp.mutable_register_client()->set_client_id(client_id);
	return arrow::Status::OK();
}

arrow::Status ClientRegistry::Unregister(const string &client_id, distributed::DistributedResponse &resp) {
	shared_ptr<ClientRegistration> removed;
	{
		const concurrency::unique_lock<concurrency::shared_mutex> lock(mutex);
		auto entry = clients.find(client_id);
		if (entry != clients.end()) {
			removed = std::move(entry->second);
			clients.erase(entry);
		}
	}
	resp.set_success(true);
	resp.mutable_unregister_client();
	return arrow::Status::OK();
}

void ClientRegistry::Clear() {
	unordered_map<string, shared_ptr<ClientRegistration>> removed_clients;
	shared_ptr<ServedDatabase> removed_default;
	const concurrency::unique_lock<concurrency::shared_mutex> lock(mutex);
	removed_clients.swap(clients);
	removed_default = std::move(default_database);
	databases.clear();
}

bool ClientRegistry::Lookup(const string &client_id, shared_ptr<ClientRegistration> &registration) {
	const concurrency::shared_lock<concurrency::shared_mutex> lock(mutex);
	auto entry = clients.find(client_id);
	if (entry == clients.end()) {
		return false;
	}
	registration = entry->second;
	return true;
}

bool ClientRegistry::Authorize(const string &client_id, distributed::ClientRole required_role,
                               shared_ptr<ClientRegistration> &registration, distributed::DistributedResponse &resp) {
	if (!Lookup(client_id, registration)) {
		resp.set_success(false);
		resp.set_error_message(NOT_REGISTERED_ERROR);
		return false;
	}
	if (required_role == distributed::CLIENT_ROLE_READ_WRITE &&
	    registration->role != distributed::CLIENT_ROLE_READ_WRITE) {
		resp.set_success(false);
		resp.set_error_message("Duckherder client is read-only");
		return false;
	}
	Touch(registration);
	return true;
}

void ClientRegistry::Touch(const shared_ptr<ClientRegistration> &registration) {
	registration->last_seen = GetSteadyNowMilliSecSinceEpoch();
}

void ClientRegistry::PruneExpired(vector<shared_ptr<ClientRegistration>> &expired) {
	const auto expiration = GetSteadyNowMilliSecSinceEpoch() - test_state.GetClientLeaseTimeout().count();
	for (auto entry = clients.begin(); entry != clients.end();) {
		auto current = entry++;
		if (current->second->last_seen.load() < expiration) {
			expired.emplace_back(std::move(current->second));
			clients.erase(current);
		}
	}
	for (auto entry = databases.begin(); entry != databases.end();) {
		auto current = entry++;
		if (current->second.expired()) {
			databases.erase(current);
		}
	}
}

} // namespace duckdb
