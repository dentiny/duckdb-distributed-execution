#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

// Physical operator that intercepts DELETE and sends to distributed server.
class PhysicalDistributedDelete : public PhysicalOperator {
public:
	PhysicalDistributedDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &table_p,
	                          PhysicalOperator &child_operator, idx_t row_id_index, idx_t estimated_cardinality,
	                          bool return_chunk);

	TableCatalogEntry &table;
	PhysicalOperator &child;
	idx_t row_id_index;
	bool return_chunk;

public:
	// Sink interface.
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

	// Source interface.
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
