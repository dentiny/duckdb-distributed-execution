#include "server/driver/served_database.hpp"

#include "duckdb/common/exception.hpp"
#include "server/driver/client_registration.hpp"
#include "server/object_storage_database.hpp"

namespace duckdb {

ServedDatabase::ServedDatabase(shared_ptr<DuckDB> duckling) : duckling_instance(std::move(duckling)) {
}

ServedDatabase::ServedDatabase(distributed::StorageConfig config_p) : config(std::move(config_p)) {
}

shared_ptr<ClientRegistration> ServedDatabase::AddClient(distributed::ClientRole role, WorkerManager &worker_manager) {
	const bool writable = role == distributed::CLIENT_ROLE_READ_WRITE;
	if (writable && has_writer) {
		throw InvalidInputException("Database %s already has a writable Duckherder client",
		                            ObjectStorageDatabase::IsConfigured(config) ? config.database_uri() : "duckling");
	}
	shared_ptr<ClientRegistration> registration;
	if (ObjectStorageDatabase::IsConfigured(config)) {
		if (!object_storage_database) {
			object_storage_database = make_shared_ptr<ObjectStorageDatabase>(config, AccessMode::READ_WRITE);
		}
		registration = make_shared_ptr<ClientRegistration>(object_storage_database, worker_manager, role, config);
	} else {
		registration = make_shared_ptr<ClientRegistration>(duckling_instance, worker_manager, role, config);
	}
	if (writable) {
		has_writer = true;
	} else {
		++reader_count;
	}
	return registration;
}

void ServedDatabase::RemoveClient(distributed::ClientRole role) {
	if (role == distributed::CLIENT_ROLE_READ_WRITE) {
		has_writer = false;
	} else {
		--reader_count;
	}
}

bool ServedDatabase::HasClients() const {
	return has_writer || reader_count > 0;
}

} // namespace duckdb
