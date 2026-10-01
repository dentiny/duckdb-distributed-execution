#include "duckherder_storage.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_remote_endpoint.hpp"
#include "duckherder_transaction_manager.hpp"

namespace duckdb {

namespace {

// Build the object storage selection from the endpoint's database name and DATA_PATH, removing the option so
// StorageManager doesn't validate it.
distributed::StorageConfig ExtractStorageConfig(const string &database_name, AttachOptions &options) {
	if (options.options.find("secret") != options.options.end()) {
		throw NotImplementedException("Duckherder ATTACH does not support SECRET yet");
	}
	string data_path;
	auto entry = options.options.find("data_path");
	if (entry != options.options.end()) {
		data_path = entry->second.ToString();
		options.options.erase(entry);
	}

	distributed::StorageConfig config;
	if (database_name.empty()) {
		if (!data_path.empty()) {
			throw InvalidInputException("Duckherder DATA_PATH requires a database name, for example "
			                            "ATTACH 'localhost:8815/db_name' (TYPE duckherder, DATA_PATH '/path')");
		}
		return config;
	}
	config.set_database_uri(StringUtil::Format("duckdb_objfs://%s", database_name));
	if (data_path.empty()) {
		config.mutable_in_memory();
	} else if (data_path.find("://") != string::npos) {
		throw NotImplementedException("Duckherder DATA_PATH only supports local paths yet, got '%s'", data_path);
	} else {
		config.mutable_local()->set_root(data_path);
	}
	return config;
}

unique_ptr<Catalog> DuckherderAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &options) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DuckherderAttach");

	if (options.options.find("server_host") != options.options.end() ||
	    options.options.find("server_port") != options.options.end()) {
		throw InvalidInputException(
		    "Duckherder server_host/server_port options are no longer supported; specify the endpoint in the "
		    "ATTACH path, for example ATTACH 'localhost:8815' (TYPE duckherder)");
	}
	auto endpoint = ParseRemoteEndpoint(info.path);
	const bool attach_read_only = options.access_mode == AccessMode::READ_ONLY;
	auto role = attach_read_only ? distributed::CLIENT_ROLE_READ_ONLY : distributed::CLIENT_ROLE_READ_WRITE;

	auto it = options.options.find("client_role");
	if (it != options.options.end()) {
		auto role_name = StringUtil::Upper(it->second.ToString());
		if (role_name == "READ_ONLY") {
			role = distributed::CLIENT_ROLE_READ_ONLY;
		} else if (role_name == "READ_WRITE") {
			if (attach_read_only) {
				throw InvalidInputException("Duckherder client_role 'read_write' conflicts with ATTACH READ_ONLY");
			}
			role = distributed::CLIENT_ROLE_READ_WRITE;
		} else {
			throw InvalidInputException("Duckherder client_role must be 'read_only' or 'read_write'");
		}
	}

	// Remove our custom options so StorageManager doesn't validate them.
	options.options.erase("client_role");
	auto storage_config = ExtractStorageConfig(endpoint.database_name, options);

	// DuckCatalog is only the client-side metadata cache. Never persist its entries or table storage to the ATTACH
	// path. Its backing storage must remain writable even when the remote attachment itself is READ_ONLY.
	info.path = ":memory:";
	options.access_mode = AccessMode::READ_WRITE;

	auto catalog = make_uniq<DuckherderCatalog>(db, std::move(endpoint.host), endpoint.port, role,
	                                            context.GetConnectionId(), std::move(storage_config));
	catalog->GetClient(context);
	return std::move(catalog);
}

} // namespace

unique_ptr<TransactionManager> DuckherderCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DuckherderCreateTransactionManager");
	return make_uniq<DuckherderTransactionManager>(db);
}

DuckherderStorageExtension::DuckherderStorageExtension() {
	attach = DuckherderAttach;
	create_transaction_manager = DuckherderCreateTransactionManager;
}

} // namespace duckdb
