#include "server/driver/result_merger.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/client_context.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

ResultMerger::ResultMerger(Connection &conn_p) : conn(conn_p) {
}

arrow::Status ResultMerger::CollectResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                           const vector<string> &names, const vector<LogicalType> &types,
                                           std::shared_ptr<arrow::Schema> &schema,
                                           vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	// Workers and local execution encode results with the same converter options.
	auto client_properties = conn.context->GetClientProperties();
	client_properties.arrow_lossless_conversion = true;
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);
	ARROW_ASSIGN_OR_RAISE(schema, arrow::ImportSchema(&arrow_schema));

	for (auto &stream : streams) {
		while (true) {
			ARROW_ASSIGN_OR_RAISE(auto next, stream->Next());
			if (!next.data) {
				break;
			}
			// Field metadata carries Arrow extension types, so it must match too.
			if (!next.data->schema()->Equals(*schema, /*check_metadata=*/true)) {
				return arrow::Status::Invalid("Worker result schema ", next.data->schema()->ToString(),
				                              " does not match expected schema ", schema->ToString());
			}
			batches.emplace_back(std::move(next.data));
		}
	}
	return arrow::Status::OK();
}

} // namespace duckdb
