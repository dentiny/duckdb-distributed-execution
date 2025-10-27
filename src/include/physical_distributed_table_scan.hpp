#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include <memory>

namespace duckdb {

class PhysicalDistributedTableScan : public PhysicalOperator {
public:
	PhysicalDistributedTableScan(PhysicalPlan &physical_plan, vector<LogicalType> types, const string &server_url,
	                             const string &table_name, vector<string> column_names,
	                             vector<LogicalType> column_types, idx_t estimated_cardinality);

	~PhysicalDistributedTableScan() override = default;

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
