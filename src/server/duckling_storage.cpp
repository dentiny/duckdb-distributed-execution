#include "duckling_storage.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckling_catalog.hpp"
#include "duckling_transaction_manager.hpp"

namespace duckdb {

namespace {

unique_ptr<Catalog> DucklingAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                   AttachedDatabase &db, const string &name, AttachInfo &info, AttachOptions &options) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DucklingAttach");

	// For now, Duckling is a simple no-op wrapper around DuckCatalog
	// In the future, this can be extended to support fleet distribution, monitoring, etc.
	return make_uniq<DucklingCatalog>(db);
}

unique_ptr<TransactionManager> DucklingCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                AttachedDatabase &db, Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DucklingCreateTransactionManager");
	return make_uniq<DucklingTransactionManager>(db);
}

} // namespace

DucklingStorageExtension::DucklingStorageExtension() {
	attach = DucklingAttach;
	create_transaction_manager = DucklingCreateTransactionManager;
}

} // namespace duckdb
