#include "arrow_record_batch_converter.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

namespace duckdb {

ArrowRecordBatchConverter::ArrowRecordBatchConverter(ClientContext &context_p, const arrow::RecordBatch &batch,
                                                     const vector<LogicalType> *expected_types)
    : context(context_p), owned_array(make_shared_ptr<ArrowArrayWrapper>()),
      row_count(NumericCast<idx_t>(batch.num_rows())) {
	ArrowSchemaWrapper arrow_schema;
	auto status = arrow::ExportRecordBatch(batch, &owned_array->arrow_array, &arrow_schema.arrow_schema);
	if (!status.ok()) {
		throw InvalidInputException("Exporting Arrow record batch failed: %s", status.ToString());
	}
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, arrow_schema.arrow_schema);
	auto &inferred_types = arrow_table.GetTypes();
	auto &arrow_types = arrow_table.GetColumns();
	auto column_count = NumericCast<idx_t>(batch.num_columns());

	if (inferred_types.size() != column_count ||
	    NumericCast<idx_t>(owned_array->arrow_array.n_children) != column_count) {
		throw InvalidInputException("Arrow record batch schema does not match its columns");
	}
	if (expected_types && expected_types->size() != column_count) {
		throw InvalidInputException("Expected %llu columns but Arrow batch contains %llu", expected_types->size(),
		                            column_count);
	}
	types = expected_types ? *expected_types : inferred_types;

	scan_states.reserve(column_count);
	bool needs_cast = false;
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		auto arrow_type_entry = arrow_types.find(column_idx);
		if (arrow_type_entry == arrow_types.end()) {
			throw InvalidInputException("Arrow schema is missing column %llu", column_idx);
		}
		arrow_type_entry->second->ThrowIfInvalid();
		if (!owned_array->arrow_array.children[column_idx]) {
			throw InvalidInputException("Arrow record batch is missing column %llu", column_idx);
		}
		scan_states.emplace_back(make_uniq<ArrowArrayScanState>(context));
		scan_states.back()->owned_data = owned_array;
		needs_cast = needs_cast || inferred_types[column_idx] != types[column_idx];
	}
	if (needs_cast) {
		inferred.Initialize(context, inferred_types);
	}
}

bool ArrowRecordBatchConverter::Next(DataChunk &out) {
	out.Reset();
	if (!HasNext()) {
		return false;
	}
	const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, row_count - offset);
	auto &inferred_types = arrow_table.GetTypes();
	auto &arrow_types = arrow_table.GetColumns();
	// Each vector holds its own row count, which the conversion expects to be set beforehand.
	out.SetChildCardinality(count);
	inferred.Reset();
	if (inferred.ColumnCount() > 0) {
		inferred.SetChildCardinality(count);
	}
	for (idx_t column_idx = 0; column_idx < types.size(); column_idx++) {
		const bool cast = inferred_types[column_idx] != types[column_idx];
		auto &target = cast ? inferred.data[column_idx] : out.data[column_idx];
		auto &arrow_type = *arrow_types.at(column_idx);
		auto &child_array = *owned_array->arrow_array.children[column_idx];
		auto &scan_state = *scan_states[column_idx];
		switch (arrow_type.GetPhysicalType()) {
		case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(target, child_array, offset, scan_state, count,
			                                                       arrow_type);
			break;
		case ArrowArrayPhysicalType::RUN_END_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(target, child_array, offset, scan_state, count,
			                                                          arrow_type);
			break;
		case ArrowArrayPhysicalType::DEFAULT:
			ArrowToDuckDBConversion::SetValidityMask(target, child_array, offset, count,
			                                         owned_array->arrow_array.offset, -1);
			ArrowToDuckDBConversion::ColumnArrowToDuckDB(target, child_array, offset, scan_state, count, arrow_type);
			break;
		default:
			throw NotImplementedException("Unsupported Arrow physical type");
		}
		if (cast) {
			VectorOperations::Cast(context, target, out.data[column_idx], count);
		}
	}
	offset += count;
	return true;
}

} // namespace duckdb
