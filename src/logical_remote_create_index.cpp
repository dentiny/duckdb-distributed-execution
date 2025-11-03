#include "logical_remote_create_index.hpp"

#include "distributed_create_index.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

LogicalRemoteCreateIndex::LogicalRemoteCreateIndex(unique_ptr<CreateIndexInfo> info_p, SchemaCatalogEntry &schema_p,
                                                   TableCatalogEntry &table_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), schema(schema_p), table(table_p) {
}

PhysicalOperator &LogicalRemoteCreateIndex::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Create the physical operator for remote CREATE INDEX
	// Make a copy of the info since the system might need to access the logical operator later
	auto info_copy = unique_ptr_cast<CreateInfo, CreateIndexInfo>(info->Copy());
	
	// Pass the catalog, schema, and table names instead of references to avoid dangling reference issues
	string catalog_name = schema.catalog.GetName();
	string schema_name = schema.name;
	string table_name = table.name;
	
	return planner.Make<PhysicalRemoteCreateIndex>(std::move(info_copy), std::move(catalog_name),
	                                                std::move(schema_name), std::move(table_name),
	                                                estimated_cardinality);
}

} // namespace duckdb

