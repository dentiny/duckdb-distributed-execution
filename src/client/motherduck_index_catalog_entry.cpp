#include "motherduck_index_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "motherduck_catalog.hpp"

namespace duckdb {

/*static*/ shared_ptr<DataTableInfo> MotherduckIndexCatalogEntry::GetDummyDataTableInfo(Catalog &catalog) {
	// Create a dummy DataTableInfo that can safely handle CommitDrop() and RemoveIndex() calls.
	// The TableIndexList inside will be empty, so all operations will safely no-op.
	auto &db = catalog.GetAttached();
	return make_shared_ptr<DataTableInfo>(db, /*table_io_manager_p=*/nullptr, "remote_dummy_schema",
	                                      "remote_dummy_table");
}

MotherduckIndexCatalogEntry::MotherduckIndexCatalogEntry(Catalog &motherduck_catalog_p, SchemaCatalogEntry &schema,
                                                         CreateIndexInfo &info)
    : DuckIndexEntry(motherduck_catalog_p, schema, info,
                     make_shared_ptr<IndexDataTableInfo>(GetDummyDataTableInfo(motherduck_catalog_p), info.index_name)),
      motherduck_catalog_ref(motherduck_catalog_p), remote_schema_name(schema.name), remote_table_name(info.table) {
}

MotherduckIndexCatalogEntry::~MotherduckIndexCatalogEntry() {
}

string MotherduckIndexCatalogEntry::GetSchemaName() const {
	return remote_schema_name;
}

string MotherduckIndexCatalogEntry::GetTableName() const {
	return remote_table_name;
}

void MotherduckIndexCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	// Remote indexes don't have local storage to rollback.
	// The base DuckIndexEntry::Rollback handles null info->info safely.
	DuckIndexEntry::Rollback(prev_entry);
}

unique_ptr<CatalogEntry> MotherduckIndexCatalogEntry::Copy(ClientContext &context) const {
	// For remote indexes, create a copy with the same remote info.
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateIndexInfo>();
	auto result = make_uniq<MotherduckIndexCatalogEntry>(motherduck_catalog_ref,
	                                                     const_cast<SchemaCatalogEntry &>(schema), cast_info);
	return std::move(result);
}

} // namespace duckdb
