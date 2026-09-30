#include "server/driver/served_database.hpp"

#include "duckdb/common/exception.hpp"
#include "server/driver/client_registration.hpp"
#include "server/object_storage_database.hpp"

namespace duckdb {

ServedDatabase::ServedDatabase(shared_ptr<DuckDB> duckling) : instance(std::move(duckling)), instance_writable(true) {
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
	if (!instance || (writable && !instance_writable)) {
		instance = OpenObjectStorageDatabase(config, writable ? AccessMode::READ_WRITE : AccessMode::READ_ONLY);
		instance_writable = writable;
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
	if (writer_client_id == client_id) {
		writer_client_id.clear();
	} else {
		reader_client_ids.erase(client_id);
	}
}

bool ServedDatabase::HasClients() const {
	return !writer_client_id.empty() || !reader_client_ids.empty();
}

} // namespace duckdb
