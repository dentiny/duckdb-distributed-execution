#include "logical_distributed_load.hpp"

#include "duckherder_catalog.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "physical_distributed_load.hpp"

namespace duckdb {

LogicalDistributedLoad::LogicalDistributedLoad(unique_ptr<LoadInfo> info_p, DuckherderCatalog &catalog_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), catalog(catalog_p) {
}

PhysicalOperator &LogicalDistributedLoad::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Create the physical operator for distributed LOAD.
	// Make a copy of the info since the system might need to access the logical operator later.
	auto info_copy = make_uniq<LoadInfo>(*info);

	return planner.Make<PhysicalDistributedLoad>(std::move(info_copy), catalog, estimated_cardinality);
}

} // namespace duckdb

