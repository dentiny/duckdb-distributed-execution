#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "motherduck_table.hpp"
#include "motherduck_table_metadata.hpp"

namespace duckdb {

MotherduckTable::MotherduckTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

MotherduckTable::~MotherduckTable() = default;

unique_ptr<BaseStatistics> MotherduckTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not implemented");
}

TableStorageInfo MotherduckTable::GetStorageInfo(ClientContext &context) {
	throw NotImplementedException("GetStorageInfo not implemented");
}

MotherduckTableMetadata &MotherduckTable::GetTableMetadata() {
	lock_guard<mutex> guard(lock);
	if (!metadata) {
		metadata = make_uniq<MotherduckTableMetadata>(schema.name, name);
	}
	return *metadata;
}

} // namespace duckdb
