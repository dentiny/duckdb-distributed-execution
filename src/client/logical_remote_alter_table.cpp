#include "logical_remote_alter_table.hpp"

#include "distributed_alter_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

LogicalRemoteAlterTableOperator::LogicalRemoteAlterTableOperator(unique_ptr<AlterTableInfo> info_p,
                                                                 SchemaCatalogEntry &schema_p,
                                                                 TableCatalogEntry &table_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), schema(schema_p), table(table_p) {
}

PhysicalOperator &LogicalRemoteAlterTableOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Create the physical operator for remote ALTER TABLE.
	// Make a copy of the info since the system might need to access the logical operator later.
	auto info_copy = unique_ptr_cast<AlterInfo, AlterTableInfo>(info->Copy());

	// Pass the catalog, schema, and table names instead of references to avoid dangling reference issues.
	string catalog_name = schema.catalog.GetName();
	string schema_name = schema.name;
	string table_name = table.name;

	return planner.Make<PhysicalRemoteAlterTableOperator>(std::move(info_copy), std::move(catalog_name),
	                                                      std::move(schema_name), std::move(table_name),
	                                                      estimated_cardinality);
}

} // namespace duckdb
