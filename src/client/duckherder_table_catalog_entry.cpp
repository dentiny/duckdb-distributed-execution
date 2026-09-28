#include "duckherder_table_catalog_entry.hpp"

#include "client/execution/distributed_table_scan_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckherder_catalog.hpp"

namespace duckdb {

DuckherderTableCatalogEntry::DuckherderTableCatalogEntry(DuckherderCatalog &duckherder_catalog_p,
                                                         DatabaseInstance &db_instance_p,
                                                         DuckTableEntry *duck_table_entry_p,
                                                         unique_ptr<BoundCreateTableInfo> bound_create_table_info_p)
    : DuckTableEntry(duckherder_catalog_p, duck_table_entry_p->schema, *bound_create_table_info_p,
                     duck_table_entry_p->GetStorage().shared_from_this()),
      db_instance(db_instance_p), bound_create_table_info(std::move(bound_create_table_info_p)),
      duckherder_catalog(duckherder_catalog_p) {
}

TableFunction DuckherderTableCatalogEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &bind_data) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderTableCatalogEntry::GetScanFunction");

	auto config = duckherder_catalog.GetRemoteTableConfig(schema.name, name);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Table query %s is distributed. Using remote scan from %s.", name,
	                                                 config.server_url));
	bind_data = make_uniq<DistributedTableScanBindData>(*this, config.server_url, config.remote_table_name);
	return DistributedTableScanFunction::GetFunction();
}

TableFunction DuckherderTableCatalogEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &bind_data,
                                                           const EntryLookupInfo &) {
	DUCKDB_LOG_DEBUG(db_instance, "DuckherderTableCatalogEntry::GetScanFunction");

	auto config = duckherder_catalog.GetRemoteTableConfig(schema.name, name);
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Table query %s is distributed. Using remote scan from %s.", name,
	                                                 config.server_url));
	bind_data = make_uniq<DistributedTableScanBindData>(*this, config.server_url, config.remote_table_name);
	return DistributedTableScanFunction::GetFunction();
}

} // namespace duckdb
