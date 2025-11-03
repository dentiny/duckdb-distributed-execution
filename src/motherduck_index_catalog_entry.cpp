#include "motherduck_index_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "motherduck_catalog.hpp"
#include <iostream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MotherduckIndexCatalogEntry (Local Indexes)
//===--------------------------------------------------------------------===//

// Constructor for local indexes (with storage)
MotherduckIndexCatalogEntry::MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
                                                         SchemaCatalogEntry &schema, CreateIndexInfo &info,
                                                         TableCatalogEntry &table)
    : DuckIndexEntry(motherduck_catalog_p, schema, info, table), db_instance(db_instance_p),
      motherduck_catalog_ref(motherduck_catalog_p) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Creating local MotherduckIndexCatalogEntry: %s", info.index_name));
}

MotherduckIndexCatalogEntry::~MotherduckIndexCatalogEntry() {
	std::cerr << "[~MotherduckIndexCatalogEntry] DESTRUCTOR - name=" << name << std::endl;
}

//===--------------------------------------------------------------------===//
// RemoteIndexCatalogEntry (Remote Indexes)
//===--------------------------------------------------------------------===//

RemoteIndexCatalogEntry::RemoteIndexCatalogEntry(Catalog &motherduck_catalog_p, SchemaCatalogEntry &schema,
                                                 CreateIndexInfo &info)
    : IndexCatalogEntry(motherduck_catalog_p, schema, info), motherduck_catalog_ref(motherduck_catalog_p),
      remote_schema_name(info.schema), remote_table_name(info.table) {
	std::cerr << "[RemoteIndexCatalogEntry] Creating REMOTE index - name=" << info.index_name << std::endl;
}

RemoteIndexCatalogEntry::~RemoteIndexCatalogEntry() {
	std::cerr << "[~RemoteIndexCatalogEntry] DESTRUCTOR - name=" << name << std::endl;
}

string RemoteIndexCatalogEntry::GetSchemaName() const {
	return remote_schema_name;
}

string RemoteIndexCatalogEntry::GetTableName() const {
	return remote_table_name;
}

void RemoteIndexCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	std::cerr << "[RemoteIndexCatalogEntry::Rollback] ENTER - name=" << name << std::endl;
	// Remote indexes don't have local storage to rollback
	// This is intentionally empty
	std::cerr << "[RemoteIndexCatalogEntry::Rollback] EXIT - nothing to rollback for remote index" << std::endl;
}

unique_ptr<CatalogEntry> RemoteIndexCatalogEntry::Copy(ClientContext &context) const {
	std::cerr << "[RemoteIndexCatalogEntry::Copy] ENTER - name=" << name << std::endl;
	// For remote indexes, create a copy with the same remote info
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateIndexInfo>();
	auto result = make_uniq<RemoteIndexCatalogEntry>(motherduck_catalog_ref,
	                                                  const_cast<SchemaCatalogEntry&>(schema), cast_info);
	std::cerr << "[RemoteIndexCatalogEntry::Copy] Created remote copy" << std::endl;
	return std::move(result);
}

} // namespace duckdb
