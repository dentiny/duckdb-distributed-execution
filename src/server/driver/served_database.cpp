#include "server/driver/served_database.hpp"

#include "server/driver/client_registration.hpp"
#include "server/object_storage_database.hpp"

namespace duckdb {

ServedDatabase::ServedDatabase(distributed::StorageConfig config_p) : config(std::move(config_p)) {
}

arrow::Result<shared_ptr<ClientRegistration>> ServedDatabase::AddClient(distributed::ClientRole role,
                                                                        WorkerManager &worker_manager) {
	const bool writable = role == distributed::CLIENT_ROLE_READ_WRITE;
	if (writable && has_writer) {
		return arrow::Status::Invalid("Database ", config.database_uri(), " already has a writable Duckherder client");
	}
	if (!database) {
		auto database_result = ObjectStorageDatabase::Create(config, AccessMode::READ_WRITE);
		if (!database_result.ok()) {
			return database_result.status();
		}
		database = std::move(database_result).ValueOrDie();
	}
	auto connection_result = database->Connect();
	if (!connection_result.ok()) {
		return connection_result.status();
	}
	auto registration = make_shared_ptr<ClientRegistration>(*database, std::move(connection_result).ValueOrDie(),
	                                                        worker_manager, role, config);
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

bool ServedDatabase::IsDefault() const {
	return ObjectStorageDatabase::IsDefaultURI(config.database_uri());
}

} // namespace duckdb
