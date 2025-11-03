#include "motherduck_index_catalog_entry.hpp"

#include <iostream>
#include "duckdb/logging/logger.hpp"
#include "motherduck_catalog.hpp"

namespace duckdb {

// Constructor for local indexes (with storage)
MotherduckIndexCatalogEntry::MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
                                                         SchemaCatalogEntry &schema, CreateIndexInfo &info,
                                                         TableCatalogEntry &table)
    : DuckIndexEntry(motherduck_catalog_p, schema, info, table), db_instance(db_instance_p),
      motherduck_catalog_ref(motherduck_catalog_p), is_remote(false) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Creating local MotherduckIndexCatalogEntry: %s", info.index_name));
}

// Constructor for remote indexes (without storage)
MotherduckIndexCatalogEntry::MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, DatabaseInstance &db_instance_p,
                                                         SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : DuckIndexEntry(motherduck_catalog_p, schema, info, shared_ptr<IndexDataTableInfo>(nullptr)),
      db_instance(db_instance_p), motherduck_catalog_ref(motherduck_catalog_p), is_remote(true),
      remote_schema_name(info.schema), remote_table_name(info.table) {
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Creating remote MotherduckIndexCatalogEntry: %s", info.index_name));
}

string MotherduckIndexCatalogEntry::GetSchemaName() const {
	if (is_remote) {
		return remote_schema_name;
	}
	return DuckIndexEntry::GetSchemaName();
}

string MotherduckIndexCatalogEntry::GetTableName() const {
	if (is_remote) {
		return remote_table_name;
	}
	return DuckIndexEntry::GetTableName();
}

void MotherduckIndexCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	std::cerr << "[MotherduckIndexCatalogEntry::Rollback] ENTER - name=" << name << " is_remote=" << is_remote << std::endl;
	if (is_remote) {
		// Remote indexes don't have local storage to rollback
		std::cerr << "[MotherduckIndexCatalogEntry::Rollback] Skipping rollback for remote index" << std::endl;
		return;
	}
	std::cerr << "[MotherduckIndexCatalogEntry::Rollback] Calling DuckIndexEntry::Rollback..." << std::endl;
	DuckIndexEntry::Rollback(prev_entry);
	std::cerr << "[MotherduckIndexCatalogEntry::Rollback] EXIT" << std::endl;
}

void MotherduckIndexCatalogEntry::CommitDrop() {
	std::cerr << "[MotherduckIndexCatalogEntry::CommitDrop] ENTER - name=" << name << " is_remote=" << is_remote << std::endl;
	if (is_remote) {
		// Remote indexes don't have local storage to drop
		std::cerr << "[MotherduckIndexCatalogEntry::CommitDrop] Skipping commit drop for remote index" << std::endl;
		return;
	}
	std::cerr << "[MotherduckIndexCatalogEntry::CommitDrop] Calling DuckIndexEntry::CommitDrop..." << std::endl;
	DuckIndexEntry::CommitDrop();
	std::cerr << "[MotherduckIndexCatalogEntry::CommitDrop] EXIT" << std::endl;
}

} // namespace duckdb
