#include "arrow_utils.hpp"

#include "arrow_record_batch_converter.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <arrow/array.h>
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

LogicalType ArrowTypeToDuckDBType(ClientContext &context, const std::shared_ptr<arrow::DataType> &arrow_type) {
	auto schema = arrow::schema({arrow::field("value", arrow_type)});
	auto arrow_table = GetArrowTableSchema(context, *schema);
	return arrow_table.GetTypes()[0];
}

void ConvertArrowArrayToDuckDBVector(ClientContext &context, const std::shared_ptr<arrow::Array> &arrow_array,
                                     Vector &duckdb_vector, const LogicalType &type, idx_t num_rows) {
	if (num_rows > STANDARD_VECTOR_SIZE) {
		throw InvalidInputException("Cannot convert %llu rows into one vector", num_rows);
	}
	auto schema = arrow::schema({arrow::field("value", arrow_array->type())});
	auto batch = arrow::RecordBatch::Make(std::move(schema), NumericCast<int64_t>(num_rows), {arrow_array});
	vector<LogicalType> expected_types {type};
	ArrowRecordBatchConverter converter(context, *batch, &expected_types);
	DataChunk chunk;
	chunk.Initialize(context, converter.GetTypes());
	converter.Next(chunk);
	duckdb_vector.Reference(chunk.data[0]);
}

unique_ptr<QueryResult> MakeArrowResult(ClientContext &context, StatementType statement_type,
                                        vector<std::shared_ptr<arrow::RecordBatch>> batches,
                                        const std::shared_ptr<arrow::Schema> &schema,
                                        const vector<LogicalType> *expected_types) {
	vector<Identifier> names;
	vector<LogicalType> types;
	if (expected_types) {
		types = *expected_types;
	}
	if (schema) {
		auto arrow_table = GetArrowTableSchema(context, *schema);
		names = StringsToIdentifiers(arrow_table.GetNames());
		if (!expected_types) {
			types = arrow_table.GetTypes();
		}
	} else if (expected_types) {
		names.resize(types.size());
	}

	auto collection = types.empty() ? make_uniq<ColumnDataCollection>(Allocator::Get(context), types)
	                                : make_uniq<ColumnDataCollection>(context, types);
	DataChunk chunk;
	chunk.Initialize(context, types);
	for (auto &batch : batches) {
		ArrowRecordBatchConverter converter(context, *batch, &types);
		while (converter.Next(chunk)) {
			collection->Append(chunk);
		}
		batch.reset();
	}
	return make_uniq<QueryResult>(statement_type, StatementProperties(), names, std::move(collection),
	                              ClientProperties());
}

arrow::Status QueryResultToArrowBatches(QueryResult &result, std::shared_ptr<arrow::Schema> &schema,
                                        vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	auto client_properties = result.client_properties;
	client_properties.arrow_lossless_conversion = true;
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, result.GetTypes(), IdentifiersToStrings(result.GetNames()),
	                              client_properties);
	ARROW_ASSIGN_OR_RAISE(schema, arrow::ImportSchema(&arrow_schema));

	auto extension_types =
	    ArrowTypeExtensionData::GetExtensionTypes(*client_properties.client_context, result.GetTypes());
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		ArrowArray arrow_array;
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

namespace {

class ChunkArrowReader : public arrow::RecordBatchReader {
public:
	ChunkArrowReader(unique_ptr<Connection> connection_p, ChunkFetcher fetch_p, vector<LogicalType> types_p,
	                 ClientProperties client_properties_p, std::shared_ptr<arrow::Schema> schema_p)
	    : connection(std::move(connection_p)), fetch(std::move(fetch_p)), types(std::move(types_p)),
	      client_properties(std::move(client_properties_p)), schema_(std::move(schema_p)),
	      extension_types(ArrowTypeExtensionData::GetExtensionTypes(*client_properties.client_context, types)) {
	}

	std::shared_ptr<arrow::Schema> schema() const override {
		return schema_;
	}

	// Batches of a row group instead of one vector each, since every Arrow batch costs per-message overhead on each
	// hop back to the client.
	arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
		try {
			// The appender grows up to a row group, so results of a few rows do not allocate one.
			ArrowAppender appender(types, STANDARD_VECTOR_SIZE, client_properties, extension_types);
			while (appender.RowCount() < DEFAULT_ROW_GROUP_SIZE) {
				if (!chunk || chunk_offset == chunk->size()) {
					chunk = fetch();
					chunk_offset = 0;
					if (!chunk || chunk->size() == 0) {
						chunk.reset();
						break;
					}
				}
				auto count =
				    MinValue<idx_t>(DEFAULT_ROW_GROUP_SIZE - appender.RowCount(), chunk->size() - chunk_offset);
				appender.Append(*chunk, chunk_offset, chunk_offset + count, chunk->size());
				chunk_offset += count;
			}
			if (appender.RowCount() == 0) {
				batch->reset();
				return arrow::Status::OK();
			}
			auto arrow_array = appender.Finalize();
			ARROW_ASSIGN_OR_RAISE(*batch, arrow::ImportRecordBatch(&arrow_array, schema_));
			return arrow::Status::OK();
		} catch (const std::exception &ex) {
			ErrorData error(ex);
			return arrow::Status::Invalid("Task execution failed: ", error.Message());
		}
	}

private:
	// Destroyed last, as fetch may read through its context.
	unique_ptr<Connection> connection;
	ChunkFetcher fetch;
	vector<LogicalType> types;
	ClientProperties client_properties;
	std::shared_ptr<arrow::Schema> schema_;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
	// The fetched chunk whose rows from chunk_offset on are not in a batch yet.
	unique_ptr<DataChunk> chunk;
	idx_t chunk_offset = 0;
};

} // namespace

arrow::Status ChunksToArrowReader(unique_ptr<Connection> connection, const vector<LogicalType> &types,
                                  const vector<string> &names, ClientProperties client_properties, ChunkFetcher fetch,
                                  std::shared_ptr<arrow::RecordBatchReader> &reader) {
	if (!client_properties.client_context) {
		client_properties.client_context = connection->context.get();
	}
	client_properties.arrow_lossless_conversion = true;

	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);
	ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&arrow_schema));
	reader = std::make_shared<ChunkArrowReader>(std::move(connection), std::move(fetch), types,
	                                            std::move(client_properties), std::move(schema));
	return arrow::Status::OK();
}

arrow::Status QueryResultToArrowReader(unique_ptr<Connection> connection, unique_ptr<QueryResult> result,
                                       std::shared_ptr<arrow::RecordBatchReader> &reader) {
	auto types = result->GetTypes();
	auto names = IdentifiersToStrings(result->GetNames());
	auto client_properties = result->client_properties;
	auto stream = make_shared_ptr<QueryResultStream<>>(std::move(result));
	return ChunksToArrowReader(
	    std::move(connection), types, names, std::move(client_properties),
	    [stream]() {
		    auto chunk = stream->Fetch();
		    if (!chunk && stream->HasError()) {
			    stream->GetErrorObject().Throw();
		    }
		    return chunk;
	    },
	    reader);
}

} // namespace duckdb
