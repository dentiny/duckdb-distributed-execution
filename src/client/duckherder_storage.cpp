#include "duckherder_storage.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_transaction_manager.hpp"

namespace duckdb {

namespace {

unique_ptr<Catalog> DuckherderAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &options) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DuckherderAttach");

	// Extract server configuration from ATTACH DATABASE options.
	string server_host = "localhost";
	int server_port = 8815;
	const bool attach_read_only = options.access_mode == AccessMode::READ_ONLY;
	auto role = attach_read_only ? distributed::CLIENT_ROLE_READ_ONLY : distributed::CLIENT_ROLE_READ_WRITE;

	auto it = options.options.find("server_host");
	if (it != options.options.end()) {
		server_host = it->second.ToString();
	}

	it = options.options.find("server_port");
	if (it != options.options.end()) {
		server_port = it->second.GetValue<int32_t>();
	}

	it = options.options.find("client_role");
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
	options.options.erase("server_host");
	options.options.erase("server_port");
	options.options.erase("client_role");

	auto catalog =
	    make_uniq<DuckherderCatalog>(db, std::move(server_host), server_port, role, context.GetConnectionId());
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
