#include "duckherder_table_catalog_entry.hpp"

#include "client/execution/distributed_table_scan_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_schema_catalog_entry.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

DuckherderTableCatalogEntry::DuckherderTableCatalogEntry(DuckherderCatalog &duckherder_catalog,
                                                         DuckTableEntry *duck_table_entry_p,
                                                         unique_ptr<BoundCreateTableInfo> bound_create_table_info_p)
    : DuckTableEntry(duckherder_catalog, duck_table_entry_p->schema, *bound_create_table_info_p,
                     duck_table_entry_p->GetStorage().shared_from_this()),
      bound_create_table_info(std::move(bound_create_table_info_p)) {
}

TableFunction DuckherderTableCatalogEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &bind_data) {
	bind_data = make_uniq<DistributedTableScanBindData>(*this, QualifiedRemoteName(schema.name, name));
	return DistributedTableScanFunction::GetFunction();
}

TableStorageInfo DuckherderTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	auto result = DuckTableEntry::GetStorageInfo(context);
	auto remote_indexes = schema.Cast<DuckherderSchemaCatalogEntry>().GetRemoteIndexes(name);
	result.index_info.insert(result.index_info.end(), remote_indexes.begin(), remote_indexes.end());
	return result;
}

} // namespace duckdb
