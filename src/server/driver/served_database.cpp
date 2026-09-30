#include "server/driver/served_database.hpp"

#include "duckdb/common/exception.hpp"
#include "server/driver/client_registration.hpp"
#include "server/object_storage_database.hpp"

namespace duckdb {

ServedDatabase::ServedDatabase(shared_ptr<DuckDB> duckling)
    : reader_instance(duckling), writer_instance(std::move(duckling)) {
}

ServedDatabase::ServedDatabase(distributed::StorageConfig config_p) : config(std::move(config_p)) {
}

shared_ptr<ClientRegistration> ServedDatabase::AddClient(const string &client_id, distributed::ClientRole role,
                                                         WorkerManager &worker_manager) {
	const bool writable = role == distributed::CLIENT_ROLE_READ_WRITE;
	if (writable && !writer_client_id.empty()) {
		throw InvalidInputException("Database %s already has a writable Duckherder client",
		                            HasObjectStorage(config) ? config.database_uri() : "duckling");
	}
	auto &instance = writable ? writer_instance : reader_instance;
	if (!instance) {
		instance = OpenObjectStorageDatabase(config, writable ? AccessMode::READ_WRITE : AccessMode::READ_ONLY);
	}
	auto registration = make_shared_ptr<ClientRegistration>(instance, worker_manager, role, config);
	if (writable) {
		writer_client_id = client_id;
	} else {
		reader_client_ids.insert(client_id);
	}
	return registration;
}

void ServedDatabase::RemoveClient(const string &client_id) {
	// The Duckling instance is owned by the server and outlives its clients.
	const bool owns_instances = HasObjectStorage(config);
	if (writer_client_id == client_id) {
		writer_client_id.clear();
		if (owns_instances) {
			writer_instance.reset();
		}
	} else if (reader_client_ids.erase(client_id) > 0 && reader_client_ids.empty() && owns_instances) {
		reader_instance.reset();
	}
}

bool ServedDatabase::HasClients() const {
	return !writer_client_id.empty() || !reader_client_ids.empty();
}

} // namespace duckdb
