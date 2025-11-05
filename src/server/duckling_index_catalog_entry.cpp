#include "duckling_index_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckling_catalog.hpp"

namespace duckdb {

/*static*/ shared_ptr<DataTableInfo> DucklingIndexCatalogEntry::GetDummyDataTableInfo(Catalog &catalog) {
	// Create a dummy DataTableInfo for index operations.
	auto &db = catalog.GetAttached();
	return make_shared_ptr<DataTableInfo>(db, /*table_io_manager_p=*/nullptr, "duckling_schema", "duckling_table");
}

DucklingIndexCatalogEntry::DucklingIndexCatalogEntry(Catalog &duckling_catalog_p, SchemaCatalogEntry &schema,
                                                     CreateIndexInfo &info)
    : DuckIndexEntry(duckling_catalog_p, schema, info,
                     make_shared_ptr<IndexDataTableInfo>(GetDummyDataTableInfo(duckling_catalog_p), info.index_name)),
      duckling_catalog_ref(duckling_catalog_p), schema_name(schema.name), table_name(info.table) {
}

DucklingIndexCatalogEntry::~DucklingIndexCatalogEntry() {
}

string DucklingIndexCatalogEntry::GetSchemaName() const {
	return schema_name;
}

string DucklingIndexCatalogEntry::GetTableName() const {
	return table_name;
}

void DucklingIndexCatalogEntry::Rollback(CatalogEntry &prev_entry) {
	DuckIndexEntry::Rollback(prev_entry);
}

unique_ptr<CatalogEntry> DucklingIndexCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateIndexInfo>();
	auto result =
	    make_uniq<DucklingIndexCatalogEntry>(duckling_catalog_ref, const_cast<SchemaCatalogEntry &>(schema), cast_info);
	return std::move(result);
}

} // namespace duckdb
