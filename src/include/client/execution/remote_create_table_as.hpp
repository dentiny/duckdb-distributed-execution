#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

class DuckherderCatalog;
class DuckherderSchemaCatalogEntry;
class LogicalCreateTable;

// Executes CREATE TABLE AS on the control node and caches the created table metadata locally.
class PhysicalRemoteCreateTableAs : public PhysicalOperator {
public:
	PhysicalRemoteCreateTableAs(PhysicalPlan &physical_plan, LogicalCreateTable &op, DuckherderCatalog &catalog_p,
	                            DuckherderSchemaCatalogEntry &schema_p, unique_ptr<BoundCreateTableInfo> info_p,
	                            string sql_p);

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

private:
	DuckherderCatalog &catalog;
	DuckherderSchemaCatalogEntry &schema;
	unique_ptr<BoundCreateTableInfo> info;
	string sql;
};

} // namespace duckdb
