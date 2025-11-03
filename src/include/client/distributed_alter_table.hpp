// Physical operation to alter remote tables.

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

class PhysicalRemoteAlterTableOperator : public PhysicalOperator {
public:
	PhysicalRemoteAlterTableOperator(PhysicalPlan &physical_plan, unique_ptr<AlterTableInfo> info_p,
	                                 string catalog_name_p, string schema_name_p, string table_name_p,
	                                 idx_t estimated_cardinality);

	unique_ptr<AlterTableInfo> info;
	string catalog_name;
	string schema_name;
	string table_name;

public:
	// Source interface.
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
