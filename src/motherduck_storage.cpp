#include "motherduck_storage.hpp"

#include "motherduck_catalog.hpp"
#include "motherduck_transaction_manager.hpp"

namespace duckdb {

unique_ptr<Catalog> MotherduckAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &options) {
	string uri;
	string database;
	for (auto &entry : info.options) {
		auto key = StringUtil::Lower(entry.first);
		if (key == "type" || key == "read_only") {
			continue;
		} else if (key == "uri") {
			uri = entry.second.ToString();
		} else if (key == "database") {
			database = entry.second.ToString();
		} else {
			throw NotImplementedException("Unsupported option %s", entry.first);
		}
	}
	if (uri.empty()) {
		throw InvalidInputException("Missing required option URI");
	}
	if (database.empty()) {
		throw InvalidInputException("Missing required option DATABASE");
	}
	return make_uniq<MotherduckCatalog>(db, std::move(uri), std::move(database));
}

unique_ptr<TransactionManager> MotherduckCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<MotherduckTransactionManager>(db, catalog);
}

MotherduckStorageExtension::MotherduckStorageExtension() {
	attach = MotherduckAttach;
	create_transaction_manager = MotherduckCreateTransactionManager;
}

} // namespace duckdb
