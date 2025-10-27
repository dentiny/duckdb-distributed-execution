#include "physical_distributed_table_scan.hpp"

#include "distributed_server.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalDistributedTableScan::PhysicalDistributedTableScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                           string server_url, string table_name,
                                                           vector<string> column_names,
                                                           vector<LogicalType> column_types,
                                                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      server_url(std::move(server_url)), table_name(std::move(table_name)), column_names(std::move(column_names)),
      column_types(std::move(column_types)) {
}

SourceResultType PhysicalDistributedTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	static DistributedServer server;

	// Check if table exists on server.
	if (!server.TableExists(table_name)) {
		chunk.SetCardinality(0);
		return SourceResultType::FINISHED;
	}

	// Get data from server.
	auto result = server.ScanTable(table_name, chunk.GetCapacity(), 0);
	if (result->HasError()) {
		throw Exception(ExceptionType::INTERNAL, "Distributed scan error: " + result->GetError());
	}

	// Convert result to chunk.
	auto data_chunk = result->Fetch();
	if (data_chunk && data_chunk->size() > 0) {
		// Copy the data from the fetched chunk to our output chunk
		chunk.Reference(*data_chunk);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

unique_ptr<GlobalSourceState> PhysicalDistributedTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalSourceState>();
}

unique_ptr<LocalSourceState> PhysicalDistributedTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                               GlobalSourceState &gstate) const {
	return make_uniq<LocalSourceState>();
}

InsertionOrderPreservingMap<string> PhysicalDistributedTableScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["server_url"] = server_url;
	result["table_name"] = table_name;
	return result;
}

} // namespace duckdb
