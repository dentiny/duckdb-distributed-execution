#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDistributedTableScan : public PhysicalOperator {
public:
	PhysicalDistributedTableScan(PhysicalPlan &physical_plan, vector<LogicalType> types, string server_url,
	                             string table_name, vector<string> column_names, vector<LogicalType> column_types,
	                             idx_t estimated_cardinality);
	~PhysicalDistributedTableScan() override = default;

	// TODO(hjiang): Implement the real remote execution.
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;

	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	string server_url;
	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
};

} // namespace duckdb
