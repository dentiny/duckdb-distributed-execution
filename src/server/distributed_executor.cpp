#include "distributed_executor.hpp"
#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <thread>
#include <iostream>

namespace duckdb {

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager, Connection &conn)
    : worker_manager(worker_manager), local_conn(conn) {
}

unique_ptr<QueryResult> DistributedExecutor::ExecuteDistributed(const string &sql) {
	// Simple check: only distribute SELECT queries
	if (!CanDistribute(sql)) {
		return nullptr; // Fall back to local execution
	}

	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		std::cerr << "[DistributedExecutor] No available workers, falling back to local execution" << std::endl;
		return nullptr; // No workers available
	}

	// Step 1: Extract table name from SQL (simple parsing)
	string table_name = ExtractTableName(sql);
	if (table_name.empty()) {
		std::cerr << "[DistributedExecutor] Failed to extract table name, falling back to local execution" << std::endl;
		return nullptr; // Can't determine table, fall back to local
	}

	// Step 2: Scan the table locally to get all data
	string scan_sql = "SELECT * FROM " + table_name;
	auto scan_result = local_conn.Query(scan_sql);
	if (scan_result->HasError()) {
		std::cerr << "[DistributedExecutor] Local scan failed: " << scan_result->GetError() << std::endl;
		return nullptr; // Error scanning, fall back to local
	}

	// Step 3: Partition the data across workers (round-robin by row)
	auto partitions = PartitionData(*scan_result, workers.size());
	if (partitions.empty()) {
		std::cerr << "[DistributedExecutor] Partitioning produced no data" << std::endl;
		return nullptr; // No data to partition
	}
	for (size_t i = 0; i < partitions.size(); i++) {
		size_t rows = 0;
		for (auto &chunk : partitions[i]) {
			rows += chunk->size();
		}
		std::cerr << "[DistributedExecutor] Partition " << i << " rows " << rows << std::endl;
	}

	std::cerr << "[DistributedExecutor] Distributing query '" << sql << "' to " << workers.size()
	          << " workers (table: " << table_name << ")" << std::endl;

	// Step 4: Send each partition to its worker and collect results
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams;

	for (size_t i = 0; i < workers.size(); i++) {
		auto *worker = workers[i];

		// Serialize partition to Arrow IPC
		string partition_data = SerializePartitionToArrowIPC(partitions[i], scan_result->types, scan_result->names);
		std::cerr << "[DistributedExecutor] Worker " << worker->worker_id << " partition size " << partition_data.size()
		          << " bytes" << std::endl;
		if (partition_data.empty()) {
			std::cerr << "[DistributedExecutor] Skipping worker " << worker->worker_id << " due to empty partition"
			          << std::endl;
			continue;
		}

		// Create partition request
		distributed::ExecutePartitionRequest req;
		req.set_sql(sql);
		req.set_partition_id(i);
		req.set_total_partitions(workers.size());
		req.set_partition_data(partition_data);

		// Execute on worker
		std::unique_ptr<arrow::flight::FlightStreamReader> stream;
		auto status = worker->client->ExecutePartition(req, stream);

		if (!status.ok()) {
			std::cerr << "[DistributedExecutor] Worker " << worker->worker_id << " failed: " << status.ToString()
			          << std::endl;
			// Worker failed, continue with others
			continue;
		}

		std::cerr << "[DistributedExecutor] Worker " << worker->worker_id << " stream ready" << std::endl;
		result_streams.push_back(std::move(stream));
	}

	// Step 5: Collect and merge results from all workers
	std::cerr << "[DistributedExecutor] Collecting results from " << result_streams.size() << " worker streams"
	          << std::endl;
	return CollectAndMergeResults(result_streams, scan_result->names, scan_result->types);
}

bool DistributedExecutor::CanDistribute(const string &sql) {
	// Simple heuristic: check if it's a SELECT query
	string sql_upper = StringUtil::Upper(sql);
	StringUtil::Trim(sql_upper);

	// Must start with SELECT
	if (!StringUtil::StartsWith(sql_upper, "SELECT")) {
		return false;
	}

	// Skip if it has ORDER BY (for now)
	if (sql_upper.find("ORDER BY") != string::npos) {
		return false;
	}

	return true;
}

string DistributedExecutor::ExtractTableName(const string &sql) {
	// Simple parser: extract table name from "SELECT ... FROM table_name ..."
	string sql_upper = StringUtil::Upper(sql);

	auto from_pos = sql_upper.find("FROM");
	if (from_pos == string::npos) {
		return "";
	}

	// Start after "FROM "
	size_t start = from_pos + 4;
	while (start < sql.length() && std::isspace(sql[start])) {
		start++;
	}

	// Find end of table name (space, comma, semicolon, or end of string)
	size_t end = start;
	while (end < sql.length() && !std::isspace(sql[end]) && sql[end] != ',' && sql[end] != ';' && sql[end] != '(' &&
	       sql[end] != ')') {
		end++;
	}

	if (end > start) {
		return sql.substr(start, end - start);
	}

	return "";
}

vector<vector<unique_ptr<DataChunk>>> DistributedExecutor::PartitionData(QueryResult &result, idx_t num_partitions) {
	vector<vector<unique_ptr<DataChunk>>> partitions(num_partitions);

	idx_t row_idx = 0;
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		// For simplicity, assign whole chunks in round-robin fashion
		// In production, we'd split chunks by rows for better balance
		idx_t partition_id = row_idx % num_partitions;
		std::cerr << "[DistributedExecutor] Assigning chunk of size " << chunk->size() << " to partition "
		          << partition_id << std::endl;
		partitions[partition_id].push_back(std::move(chunk));
		row_idx++;
	}
	std::cerr << "[DistributedExecutor] Partitioned total rows " << row_idx << std::endl;

	return partitions;
}

string DistributedExecutor::SerializePartitionToArrowIPC(vector<unique_ptr<DataChunk>> &partition,
                                                         const vector<LogicalType> &types,
                                                         const vector<string> &names) {
	if (partition.empty()) {
		std::cerr << "[DistributedExecutor] SerializePartitionToArrowIPC: empty partition" << std::endl;
		return "";
	}

	// Convert DuckDB types to Arrow schema
	ArrowSchema arrow_schema;
	ClientProperties client_props;
	client_props.client_context = local_conn.context.get();
	ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_props);
	auto schema_result = arrow::ImportSchema(&arrow_schema);
	if (!schema_result.ok()) {
		std::cerr << "[DistributedExecutor] ImportSchema failed: " << schema_result.status().ToString() << std::endl;
		return "";
	}
	auto schema = schema_result.ValueOrDie();

	// Convert each chunk to Arrow RecordBatch
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	for (auto &chunk : partition) {
		std::cerr << "[DistributedExecutor] Converting chunk of size " << chunk->size() << std::endl;
		ArrowArray arrow_array;
		auto extension_types = ArrowTypeExtensionData::GetExtensionTypes(*local_conn.context, types);
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, client_props, extension_types);

		auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
		if (!batch_result.ok()) {
			std::cerr << "[DistributedExecutor] ImportRecordBatch failed: " << batch_result.status().ToString()
			          << std::endl;
			continue;
		}
		batches.push_back(batch_result.ValueOrDie());
	}

	// Serialize to Arrow IPC format
	auto buffer_output = arrow::io::BufferOutputStream::Create().ValueOrDie();
	auto writer_result = arrow::ipc::MakeStreamWriter(buffer_output, schema);
	if (!writer_result.ok()) {
		std::cerr << "[DistributedExecutor] MakeStreamWriter failed: " << writer_result.status().ToString()
		          << std::endl;
		return "";
	}
	auto writer = writer_result.ValueOrDie();

	for (const auto &batch : batches) {
		auto status = writer->WriteRecordBatch(*batch);
		if (!status.ok()) {
			std::cerr << "[DistributedExecutor] Failed to write batch: " << status.ToString() << std::endl;
			return "";
		}
	}

	auto close_status = writer->Close();
	if (!close_status.ok()) {
		std::cerr << "[DistributedExecutor] Failed to close writer: " << close_status.ToString() << std::endl;
		return "";
	}
	auto buffer = buffer_output->Finish().ValueOrDie();
	std::cerr << "[DistributedExecutor] Serialized partition size " << buffer->size() << " bytes" << std::endl;

	return std::string(reinterpret_cast<const char *>(buffer->data()), buffer->size());
}

unique_ptr<QueryResult>
DistributedExecutor::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                            const vector<string> &names, const vector<LogicalType> &types) {

	// Create a collection to store merged results
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

	// Read results from each worker
	for (auto &stream : streams) {
		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				std::cerr << "[DistributedExecutor] Worker stream error: " << batch_result.status().ToString()
				          << std::endl;
				break;
			}

			auto batch_with_metadata = batch_result.ValueOrDie();
			if (!batch_with_metadata.data) {
				break; // End of stream
			}

			// Convert Arrow batch to DuckDB DataChunk
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types);

			auto arrow_batch = batch_with_metadata.data;
			for (int col_idx = 0; col_idx < arrow_batch->num_columns(); ++col_idx) {
				auto arrow_array = arrow_batch->column(col_idx);
				auto &duckdb_vector = chunk.data[col_idx];
				ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, types[col_idx], arrow_batch->num_rows());
			}

			chunk.SetCardinality(arrow_batch->num_rows());
			collection->Append(chunk);
		}
	}

	ClientProperties client_props;
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), client_props);
}

arrow::Status DistributedExecutor::PartitionAndDistribute(const string &sql,
                                                          vector<std::shared_ptr<arrow::RecordBatch>> &all_batches) {
	// Deprecated - using ExecuteDistributed instead
	return arrow::Status::OK();
}

unique_ptr<QueryResult>
DistributedExecutor::CollectAndMerge(const vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	// Deprecated - using CollectAndMergeResults instead
	vector<string> names;
	vector<LogicalType> types;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	ClientProperties client_props;
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), names,
	                                          std::move(collection), client_props);
}

} // namespace duckdb
