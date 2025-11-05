#include "duckherder_storage.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
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
	string server_db_path;

	auto it = options.options.find("server_host");
	if (it != options.options.end()) {
		server_host = it->second.ToString();
	}

	it = options.options.find("server_port");
	if (it != options.options.end()) {
		server_port = it->second.GetValue<int32_t>();
	}

	it = options.options.find("server_db_path");
	if (it != options.options.end()) {
		server_db_path = it->second.ToString();
	}

	// Remove our custom options so StorageManager doesn't validate them.
	options.options.erase("server_host");
	options.options.erase("server_port");
	options.options.erase("server_db_path");

	return make_uniq<DuckherderCatalog>(db, std::move(server_host), server_port, std::move(server_db_path));
}

}  // namespace

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
