#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

// Forward declaration.
class MotherduckTableMetadata;

class MotherduckTable : public TableCatalogEntry {
public:
	MotherduckTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

	~MotherduckTable();

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	MotherduckTableMetadata &GetTableMetadata();

private:
	mutex lock;
	unique_ptr<MotherduckTableMetadata> metadata;
};

} // namespace duckdb
