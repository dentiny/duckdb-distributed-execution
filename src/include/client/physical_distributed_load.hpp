#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"

namespace duckdb {

class DuckherderCatalog;

//! PhysicalDistributedLoad represents an extension LOAD operation that also loads on the server
class PhysicalDistributedLoad : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalDistributedLoad(PhysicalPlan &physical_plan, unique_ptr<LoadInfo> info_p, DuckherderCatalog &catalog_p,
	                        idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BOOLEAN},
	                       estimated_cardinality),
	      info(std::move(info_p)), catalog(catalog_p) {
	}

	unique_ptr<LoadInfo> info;
	DuckherderCatalog &catalog;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb

