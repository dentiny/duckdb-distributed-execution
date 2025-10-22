#include "motherduck_storage.hpp"

#include "motherduck_catalog.hpp"
#include "motherduck_transaction_manager.hpp"

namespace duckdb {

unique_ptr<Catalog> MotherduckAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &options) {
	return make_uniq<MotherduckCatalog>(db);
}

unique_ptr<TransactionManager> MotherduckCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<MotherduckTransactionManager>(db);
}

MotherduckStorageExtension::MotherduckStorageExtension() {
	attach = MotherduckAttach;
	create_transaction_manager = MotherduckCreateTransactionManager;
}

} // namespace duckdb
