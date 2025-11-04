#pragma once

#include "duckdb/parser/parsed_data/load_info.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

class DuckherderCatalog;

//! LogicalDistributedLoad represents loading an extension on both client and server
class LogicalDistributedLoad : public LogicalExtensionOperator {
public:
	LogicalDistributedLoad(unique_ptr<LoadInfo> info_p, DuckherderCatalog &catalog_p);

	unique_ptr<LoadInfo> info;
	DuckherderCatalog &catalog;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	string GetExtensionName() const override {
		return "duckherder_distributed_load";
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::BOOLEAN};
	}
};

} // namespace duckdb

