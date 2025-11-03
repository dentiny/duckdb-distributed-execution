#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

// Physical operator for CREATE INDEX on remote tables.
// This operator sends the CREATE INDEX statement to the remote server
// without scanning the table locally.
class PhysicalRemoteCreateIndex : public PhysicalOperator {
public:
	PhysicalRemoteCreateIndex(PhysicalPlan &physical_plan, unique_ptr<CreateIndexInfo> info_p,
	                          string catalog_name_p, string schema_name_p, string table_name_p,
	                          idx_t estimated_cardinality);

	unique_ptr<CreateIndexInfo> info;
	string catalog_name;
	string schema_name;
	string table_name;
	mutable bool executed;

public:
	// Source interface - execute the remote CREATE INDEX and return immediately
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb

