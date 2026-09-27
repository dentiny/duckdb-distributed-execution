#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class ClientContext;
class DuckherderCatalog;
class TableCatalogEntry;

// Rewrites client catalog references to the corresponding control-node tables.
string RewriteRemoteDMLStatement(ClientContext &context, DuckherderCatalog &catalog);

// Executes one complete DML statement on the client's control-node connection.
class PhysicalRemoteDML : public PhysicalOperator {
public:
	PhysicalRemoteDML(PhysicalPlan &physical_plan, PhysicalOperatorType operator_type, vector<LogicalType> types,
	                  TableCatalogEntry &table_p, string sql_p, idx_t estimated_cardinality);

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

private:
	TableCatalogEntry &table;
	string sql;
};

} // namespace duckdb
