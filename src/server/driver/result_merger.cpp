#include "server/driver/result_merger.hpp"

#include "arrow_utils.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "server/driver/query_plan_analyzer.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

ResultMerger::ResultMerger(Connection &conn_p) : conn(conn_p) {
}

unique_ptr<QueryResult> ResultMerger::MergePartialAggregates(const vector<arrow::RecordBatchVector> &task_batches,
                                                             const vector<string> &partial_names,
                                                             const vector<LogicalType> &partial_types,
                                                             const vector<string> &output_names,
                                                             const vector<LogicalType> &output_types,
                                                             const string &final_sql) {
	const string temp_table_name = QueryPlanAnalyzer::PARTIAL_TABLE_NAME;
	vector<string> columns;
	for (idx_t idx = 0; idx < partial_names.size(); ++idx) {
		columns.push_back(SQLIdentifier::ToString(partial_names[idx]) + " " + partial_types[idx].ToString());
	}
	auto create_result = conn.Query(StringUtil::Format("CREATE OR REPLACE TEMPORARY TABLE %s (%s)", temp_table_name,
	                                                   StringUtil::Join(columns, ", ")));
	if (create_result->HasError()) {
		throw IOException("Failed to create temp table: %s", create_result->GetError());
	}

	// Append whole chunks; a per-row INSERT statement costs a full parse, bind and execute.
	Appender appender(conn, TEMP_CATALOG, DEFAULT_SCHEMA, temp_table_name);
	for (const auto &batches : task_batches) {
		for (const auto &batch : batches) {
			DataChunk chunk;
			ArrowRecordBatchToDataChunk(*conn.context, *batch, chunk, &partial_types);
			appender.AppendDataChunk(chunk);
		}
	}
	appender.Close();

	vector<string> final_columns;
	vector<string> outputs;
	for (idx_t idx = 0; idx < output_names.size(); ++idx) {
		final_columns.push_back(StringUtil::Format("__o%llu", idx));
		outputs.push_back(StringUtil::Format("CAST(__o%llu AS %s) AS %s", idx, output_types[idx].ToString(),
		                                     SQLIdentifier::ToString(output_names[idx])));
	}
	auto result = conn.Query(StringUtil::Format("WITH __final(%s) AS (%s) SELECT %s FROM __final",
	                                            StringUtil::Join(final_columns, ", "), final_sql,
	                                            StringUtil::Join(outputs, ", ")));
	conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));
	return std::move(result);
}

arrow::Status ResultMerger::CollectResults(vector<arrow::RecordBatchVector> &task_batches, const vector<string> &names,
                                           const vector<LogicalType> &types, std::shared_ptr<arrow::Schema> &schema,
                                           vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	// Workers and local execution encode results with the same converter options.
	auto client_properties = conn.context->GetClientProperties();
	client_properties.arrow_lossless_conversion = true;
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);
	ARROW_ASSIGN_OR_RAISE(schema, arrow::ImportSchema(&arrow_schema));

	for (auto &task_result : task_batches) {
		for (auto &batch : task_result) {
			// Field metadata carries Arrow extension types, so it must match too.
			if (!batch->schema()->Equals(*schema, /*check_metadata=*/true)) {
				return arrow::Status::Invalid("Worker result schema ", batch->schema()->ToString(),
				                              " does not match expected schema ", schema->ToString());
			}
			batches.emplace_back(std::move(batch));
		}
	}
	return arrow::Status::OK();
}

} // namespace duckdb
