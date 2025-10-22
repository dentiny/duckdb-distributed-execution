#include "motherduck_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

MotherduckTable::MotherduckTable(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
                                 shared_ptr<DataTable> inherited_storage)
    : TableCatalogEntry(catalog, schema, info.Base()),
      duck_table_entry(make_uniq<DuckTableEntry>(catalog, schema, info, std::move(inherited_storage))) {
}

MotherduckTable::~MotherduckTable() = default;

unique_ptr<BaseStatistics> MotherduckTable::GetStatistics(ClientContext &context, column_t column_id) {
	return duck_table_entry->GetStatistics(context, column_id);
}

TableStorageInfo MotherduckTable::GetStorageInfo(ClientContext &context) {
	return duck_table_entry->GetStorageInfo(context);
}

} // namespace duckdb
