#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class LogicalInsert;

// Physical operator that intercepts INSERT and sends to distributed server
class PhysicalDistributedInsert : public PhysicalOperator {
public:
	PhysicalDistributedInsert(PhysicalPlan &physical_plan, TableCatalogEntry &table, PhysicalOperator &child_operator,
	                          idx_t estimated_cardinality);

	TableCatalogEntry &table;
	PhysicalOperator &child;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	// Source interface (for returning results if needed)
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
