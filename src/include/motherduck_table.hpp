#pragma once

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

// Forward declaration.
class MotherduckTableMetadata;

class MotherduckTable : public TableCatalogEntry {
public:
	MotherduckTable(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
	                shared_ptr<DataTable> inherited_storage = nullptr);

	~MotherduckTable();

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	unique_ptr<DuckTableEntry> duck_table_entry;
};

} // namespace duckdb
