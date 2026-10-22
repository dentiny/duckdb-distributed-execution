// Physical operation to alter remote tables.

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

class PhysicalRemoteAlterTableOperator : public PhysicalOperator {
public:
	PhysicalRemoteAlterTableOperator(PhysicalPlan &physical_plan, unique_ptr<AlterTableInfo> info_p,
	                                 Identifier catalog_name_p, Identifier schema_name_p, Identifier table_name_p,
	                                 idx_t estimated_cardinality);

	unique_ptr<AlterTableInfo> info;
	Identifier catalog_name;
	Identifier schema_name;
	Identifier table_name;

public:
	// Source interface.
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
