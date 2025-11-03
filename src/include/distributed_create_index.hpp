// Physical operation to create index on remote tables.

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

class PhysicalRemoteCreateIndexOperator : public PhysicalOperator {
public:
	PhysicalRemoteCreateIndexOperator(PhysicalPlan &physical_plan, unique_ptr<CreateIndexInfo> info_p,
	                                  string catalog_name_p, string schema_name_p, string table_name_p,
	                                  idx_t estimated_cardinality);

	unique_ptr<CreateIndexInfo> info;
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
