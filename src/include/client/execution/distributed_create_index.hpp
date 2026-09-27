#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

// Executes CREATE INDEX on the control node while skipping DuckDB's client-side index-build pipeline:
// table scan -> projection -> optional filter/sort -> PhysicalCreateIndex.
//
// DuckDB exposes physical planning hooks for INSERT, UPDATE, and DELETE, but not for CREATE INDEX. We therefore replace
// CREATE INDEX during binding with this source operator, which has no local scan child. After forwarding the complete
// statement, it creates only the local catalog metadata needed for name resolution, dependency tracking, and subsequent
// DROP INDEX statements.
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
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
