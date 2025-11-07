#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

DistributedExecutor::DistributedExecutor(WorkerManager &worker_manager_p, Connection &conn_p)
    : worker_manager(worker_manager_p), conn(conn_p) {
}

unique_ptr<QueryResult> DistributedExecutor::ExecuteDistributed(const string &sql) {
	if (!CanDistribute(sql)) {
		return nullptr;
	}

	auto &db_instance = *conn.context->db;
	auto workers = worker_manager.GetAvailableWorkers();
	if (workers.empty()) {
		DUCKDB_LOG_DEBUG(db_instance, "No available workers, falling back to local execution");
		return nullptr;
	}

	unique_ptr<LogicalOperator> logical_plan;
	try {
		logical_plan = conn.ExtractPlan(sql);
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance, StringUtil::Format("Failed to extract logical plan for query '%s': %s", sql,
		                                               ex.what()));
		return nullptr;
	}
	if (!logical_plan) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("ExtractPlan returned null for query '%s', falling back to local execution", sql));
		return nullptr;
	}
	if (!IsSupportedPlan(*logical_plan)) {
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Logical plan for query '%s' contains unsupported operators", sql));
		return nullptr;
	}

	vector<string> partition_sqls;
	partition_sqls.reserve(workers.size());
	vector<string> serialized_plans;
	serialized_plans.reserve(workers.size());

	for (idx_t partition_id = 0; partition_id < workers.size(); partition_id++) {
		auto partition_sql = CreatePartitionSQL(sql, partition_id, workers.size());
		unique_ptr<LogicalOperator> partition_plan;
		try {
			partition_plan = conn.ExtractPlan(partition_sql);
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Failed to extract logical plan for partition query '%s': %s",
			                                     partition_sql, ex.what()));
			return nullptr;
		}
		if (!partition_plan) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Partition plan extraction returned null for query '%s'", partition_sql));
			return nullptr;
		}
		if (!IsSupportedPlan(*partition_plan)) {
			DUCKDB_LOG_WARN(db_instance, StringUtil::Format("Partition plan for query '%s' contains unsupported operators",
			                                             partition_sql));
			return nullptr;
		}
		serialized_plans.emplace_back(SerializeLogicalPlan(*partition_plan));
		partition_sqls.emplace_back(std::move(partition_sql));
		DUCKDB_LOG_DEBUG(db_instance,
		                StringUtil::Format("Prepared serialized plan for worker %llu (size: %llu bytes)",
		                                     static_cast<long long unsigned>(partition_id),
		                                     static_cast<long long unsigned>(serialized_plans.back().size())));
	}

	auto prepared = conn.Prepare(sql);
	if (prepared->HasError()) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("Failed to prepare distributed query '%s': %s", sql, prepared->GetError()));
		return nullptr;
	}

	vector<string> names = prepared->GetNames();
	vector<LogicalType> types = prepared->GetTypes();
	vector<string> serialized_types;
	serialized_types.reserve(types.size());
	for (auto &type : types) {
		serialized_types.emplace_back(SerializeLogicalType(type));
	}
	vector<std::unique_ptr<arrow::flight::FlightStreamReader>> result_streams;
	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Distributing query '%s' to %llu workers", sql,
	                                                 static_cast<long long unsigned>(workers.size())));

	for (idx_t i = 0; i < workers.size(); i++) {
		auto *worker = workers[i];
		distributed::ExecutePartitionRequest req;
		req.set_sql(partition_sqls[i]);
		req.set_partition_id(i);
		req.set_total_partitions(workers.size());
		req.set_serialized_plan(serialized_plans[i]);
		for (const auto &name : names) {
			req.add_column_names(name);
		}
		for (const auto &type_bytes : serialized_types) {
			req.add_column_types(type_bytes);
		}

		std::unique_ptr<arrow::flight::FlightStreamReader> stream;
		auto status = worker->client->ExecutePartition(req, stream);
		if (!status.ok()) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("Worker %s failed executing partition: %s", worker->worker_id,
			                                     status.ToString()));
			continue;
		}
		result_streams.push_back(std::move(stream));
	}

	if (result_streams.empty()) {
		DUCKDB_LOG_WARN(db_instance, StringUtil::Format("No worker streams produced results for query '%s'", sql));
		return nullptr;
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("Collecting results from %llu worker streams",
	                                                 static_cast<long long unsigned>(result_streams.size())));
	return CollectAndMergeResults(result_streams, names, types);
}

bool DistributedExecutor::CanDistribute(const string &sql) {
	string sql_upper = StringUtil::Upper(sql);
	StringUtil::Trim(sql_upper);
	if (!StringUtil::StartsWith(sql_upper, "SELECT")) {
		return false;
	}
	if (sql_upper.find(" FROM ") == string::npos) {
		return false;
	}
	auto contains_token = [&](const char *token) { return sql_upper.find(token) != string::npos; };
	if (contains_token(" JOIN") || contains_token(" GROUP ") || contains_token(" HAVING") ||
	    contains_token(" DISTINCT") || contains_token(" LIMIT ") || contains_token(" OFFSET ") ||
	    contains_token(" UNION ") || contains_token(" EXCEPT ") || contains_token(" INTERSECT ")) {
		return false;
	}
	if (contains_token(" ORDER BY")) {
		return false;
	}
	static const char *aggregate_tokens[] = {"COUNT(", "SUM(", "AVG(", "MIN(", "MAX("};
	for (auto token : aggregate_tokens) {
		if (sql_upper.find(token) != string::npos) {
			return false;
		}
	}
	return true;
}

string DistributedExecutor::CreatePartitionSQL(const string &sql, idx_t partition_id, idx_t total_partitions) {
	string trimmed = sql;
	StringUtil::RTrim(trimmed);
	bool has_semicolon = !trimmed.empty() && trimmed.back() == ';';
	if (has_semicolon) {
		trimmed.pop_back();
		StringUtil::RTrim(trimmed);
	}
	string clause = StringUtil::Format("(rowid %% %llu) = %llu",
	                                  static_cast<long long unsigned>(total_partitions),
	                                  static_cast<long long unsigned>(partition_id));
	string partition_sql = trimmed + " WHERE " + clause;
	if (has_semicolon) {
		partition_sql += ";";
	}
	return partition_sql;
}

bool DistributedExecutor::IsSupportedPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op.children.size() != 1) {
			return false;
		}
		return IsSupportedPlan(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_GET:
		return true;
	default:
		return false;
	}
}

string DistributedExecutor::SerializeLogicalPlan(LogicalOperator &op) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	op.Serialize(serializer);
	serializer.End();
	auto data_ptr = stream.GetData();
	return string(reinterpret_cast<const char *>(data_ptr), stream.GetPosition());
}

string DistributedExecutor::SerializeLogicalType(const LogicalType &type) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	type.Serialize(serializer);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

unique_ptr<QueryResult>
DistributedExecutor::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                            const vector<string> &names, const vector<LogicalType> &types) {

	auto &db_instance = *conn.context->db.get();
	// Create a collection to store merged results
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

	// Read results from each worker
	for (auto &stream : streams) {
		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				DUCKDB_LOG_WARN(db_instance,
				                StringUtil::Format("Worker stream error: %s", batch_result.status().ToString()));
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

} // namespace duckdb
