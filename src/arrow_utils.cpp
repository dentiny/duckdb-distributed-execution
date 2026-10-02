#include "arrow_utils.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

namespace duckdb {

namespace {

void ThrowOnArrowError(const arrow::Status &status, const char *operation) {
	if (!status.ok()) {
		throw InvalidInputException("%s failed: %s", operation, status.ToString());
	}
}

ArrowTableSchema GetArrowTableSchema(ClientContext &context, const arrow::Schema &schema) {
	ArrowSchemaWrapper arrow_schema;
	ThrowOnArrowError(arrow::ExportSchema(schema, &arrow_schema.arrow_schema), "Exporting Arrow schema");

	ArrowTableSchema result;
	ArrowTableFunction::PopulateArrowTableSchema(context, result, arrow_schema.arrow_schema);
	return result;
}

} // namespace

void ArrowRecordBatchToDataChunk(ClientContext &context, const arrow::RecordBatch &batch, DataChunk &out,
                                 const vector<LogicalType> *expected_types) {
	ArrowSchemaWrapper arrow_schema;
	auto owned_array = make_shared_ptr<ArrowArrayWrapper>();
	ThrowOnArrowError(arrow::ExportRecordBatch(batch, &owned_array->arrow_array, &arrow_schema.arrow_schema),
	                  "Exporting Arrow record batch");

	ArrowTableSchema arrow_table;
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, arrow_schema.arrow_schema);
	auto &inferred_types = arrow_table.GetTypes();
	auto &arrow_types = arrow_table.GetColumns();
	auto column_count = NumericCast<idx_t>(batch.num_columns());
	auto row_count = NumericCast<idx_t>(batch.num_rows());

	if (inferred_types.size() != column_count ||
	    NumericCast<idx_t>(owned_array->arrow_array.n_children) != column_count) {
		throw InvalidInputException("Arrow record batch schema does not match its columns");
	}
	if (expected_types && expected_types->size() != column_count) {
		throw InvalidInputException("Expected %llu columns but Arrow batch contains %llu", expected_types->size(),
		                            column_count);
	}

	DataChunk inferred;
	inferred.Initialize(Allocator::DefaultAllocator(), inferred_types, row_count);
	inferred.SetCardinality(row_count);

	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		auto arrow_type_entry = arrow_types.find(column_idx);
		if (arrow_type_entry == arrow_types.end()) {
			throw InvalidInputException("Arrow schema is missing column %llu", column_idx);
		}
		auto &arrow_type = *arrow_type_entry->second;
		arrow_type.ThrowIfInvalid();
		auto child_array = owned_array->arrow_array.children[column_idx];
		if (!child_array) {
			throw InvalidInputException("Arrow record batch is missing column %llu", column_idx);
		}

		ArrowArrayScanState scan_state(context);
		scan_state.owned_data = owned_array;
		switch (arrow_type.GetPhysicalType()) {
		case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(inferred.data[column_idx], *child_array, 0,
			                                                       scan_state, row_count, arrow_type);
			break;
		case ArrowArrayPhysicalType::RUN_END_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(inferred.data[column_idx], *child_array, 0,
			                                                          scan_state, row_count, arrow_type);
			break;
		case ArrowArrayPhysicalType::DEFAULT:
			ArrowToDuckDBConversion::SetValidityMask(inferred.data[column_idx], *child_array, 0, row_count,
			                                         owned_array->arrow_array.offset, -1);
			ArrowToDuckDBConversion::ColumnArrowToDuckDB(inferred.data[column_idx], *child_array, 0, scan_state,
			                                             row_count, arrow_type);
			break;
		default:
			throw NotImplementedException("Unsupported Arrow physical type");
		}
	}

	if (!expected_types) {
		out.Move(inferred);
		return;
	}

	out.Initialize(Allocator::DefaultAllocator(), *expected_types, row_count);
	out.SetCardinality(row_count);
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		if (inferred_types[column_idx] == (*expected_types)[column_idx]) {
			out.data[column_idx].Reference(inferred.data[column_idx]);
		} else {
			VectorOperations::Cast(context, inferred.data[column_idx], out.data[column_idx], row_count);
		}
	}
}

LogicalType ArrowTypeToDuckDBType(ClientContext &context, const std::shared_ptr<arrow::DataType> &arrow_type) {
	auto schema = arrow::schema({arrow::field("value", arrow_type)});
	auto arrow_table = GetArrowTableSchema(context, *schema);
	return arrow_table.GetTypes()[0];
}

void ConvertArrowArrayToDuckDBVector(ClientContext &context, const std::shared_ptr<arrow::Array> &arrow_array,
                                     Vector &duckdb_vector, const LogicalType &type, idx_t num_rows) {
	auto schema = arrow::schema({arrow::field("value", arrow_array->type())});
	auto batch = arrow::RecordBatch::Make(std::move(schema), NumericCast<int64_t>(num_rows), {arrow_array});
	vector<LogicalType> expected_types {type};
	DataChunk chunk;
	ArrowRecordBatchToDataChunk(context, *batch, chunk, &expected_types);
	duckdb_vector.Reference(chunk.data[0]);
}

unique_ptr<QueryResult> MakeArrowResult(ClientContext &context, StatementType statement_type,
                                        vector<std::shared_ptr<arrow::RecordBatch>> batches,
                                        const std::shared_ptr<arrow::Schema> &schema,
                                        const vector<LogicalType> *expected_types) {
	vector<string> names;
	vector<LogicalType> types;
	if (expected_types) {
		types = *expected_types;
	}
	if (schema) {
		auto arrow_table = GetArrowTableSchema(context, *schema);
		names = arrow_table.GetNames();
		if (!expected_types) {
			types = arrow_table.GetTypes();
		}
	} else if (expected_types) {
		names.resize(types.size());
	}

	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	for (auto &batch : batches) {
		DataChunk chunk;
		ArrowRecordBatchToDataChunk(context, *batch, chunk, expected_types ? &types : nullptr);
		collection->Append(chunk);
		batch.reset();
	}
	return make_uniq<MaterializedQueryResult>(statement_type, StatementProperties(), names, std::move(collection),
	                                          ClientProperties());
}

arrow::Status QueryResultToArrowBatches(QueryResult &result, std::shared_ptr<arrow::Schema> &schema,
                                        vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	auto client_properties = result.client_properties;
	client_properties.arrow_lossless_conversion = true;
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, client_properties);
	ARROW_ASSIGN_OR_RAISE(schema, arrow::ImportSchema(&arrow_schema));

	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		ArrowArray arrow_array;
		auto extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(*client_properties.client_context, result.types);
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, client_properties, extension_types);

		auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
		if (!batch_result.ok()) {
			return arrow::Status::Invalid("Failed to import Arrow batch: " + batch_result.status().ToString());
		}

		// TODO(hjiang): Avoid exception thrown.
		batches.emplace_back(batch_result.ValueOrDie());
	}
	return arrow::Status::OK();
}

arrow::Status QueryResultToArrowReader(QueryResult &result, ClientContext &context,
                                       std::shared_ptr<arrow::RecordBatchReader> &reader, idx_t *row_count) {
	if (!result.client_properties.client_context) {
		result.client_properties.client_context = &context;
	}
	auto client_properties = result.client_properties;
	client_properties.arrow_lossless_conversion = true;

	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.types, result.names, client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));

	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	idx_t count = 0;

	auto extension_types = ArrowTypeExtensionData::GetExtensionTypes(*client_properties.client_context, result.types);
	QueryResultChunkScanState scan_state(result);
	while (true) {
		ArrowArray arrow_array;
		idx_t fetched = 0;
		ErrorData error;
		if (!ArrowUtil::TryFetchChunk(scan_state, client_properties, DEFAULT_ROW_GROUP_SIZE, &arrow_array, fetched,
		                              error, extension_types)) {
			return arrow::Status::Invalid("Task execution failed: " + error.Message());
		}
		if (fetched == 0) {
			break;
		}

		auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
		if (!batch_result.ok()) {
			return arrow::Status::Invalid("Failed to import Arrow batch");
		}

		auto batch = batch_result.ValueOrDie();
		count += batch->num_rows();
		batches.emplace_back(batch);
	}

	ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches, schema));
	if (row_count) {
		*row_count = count;
	}
	return arrow::Status::OK();
}

} // namespace duckdb
