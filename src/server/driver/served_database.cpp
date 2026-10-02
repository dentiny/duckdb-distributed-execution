#include "server/driver/served_database.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "server/driver/client_registration.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
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
		auto &db_config = DBConfig::GetConfig(*database->GetInstance().instance);
		// Compressed materialization rewrites aggregates with internal functions, which worker fragments cannot call.
		db_config.options.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
		OptimizerExtension::Register(db_config, GetWorkerFragmentExtension());
	}
	auto connection_result = database->Connect();
	if (!connection_result.ok()) {
		return connection_result.status();
	}
	// In-memory storage is private to this control-node instance. Shared-storage readers can use worker snapshots.
	unique_ptr<Connection> executor_connection;
	if (!writable && config.storage_case() != distributed::StorageConfig::kInMemory) {
		auto executor_connection_result = database->Connect();
		if (!executor_connection_result.ok()) {
			return executor_connection_result.status();
		}
		executor_connection = std::move(executor_connection_result).ValueOrDie();
	}
	auto registration =
	    make_shared_ptr<ClientRegistration>(*database, std::move(connection_result).ValueOrDie(),
	                                        std::move(executor_connection), worker_manager, role, config);
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
