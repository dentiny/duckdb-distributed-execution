#include "server/driver/query_history.hpp"

namespace duckdb {

void QueryHistory::Record(QueryExecutionInfo info) {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	executions.emplace_back(info);
}

vector<QueryExecutionInfo> QueryHistory::Get() const {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	return executions;
}

void QueryHistory::Clear() {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	executions.clear();
}

void QueryHistory::FillStatsResponse(distributed::DistributedResponse &resp) const {
	auto query_executions = Get();
	resp.set_success(true);
	auto *stats_resp = resp.mutable_get_query_execution_stats();

	for (const auto &exec_info : query_executions) {
		auto *query_info = stats_resp->add_query_executions();
		query_info->set_sql(exec_info.sql);

		switch (exec_info.execution_mode) {
		case QueryExecutionMode::LOCAL:
			query_info->set_execution_mode("LOCAL");
			break;
		case QueryExecutionMode::DELEGATED:
			query_info->set_execution_mode("DELEGATED");
			break;
		case QueryExecutionMode::ROW_GROUP_PARTITION:
			query_info->set_execution_mode("ROW_GROUP_PARTITION");
			break;
		}

		switch (exec_info.merge_strategy) {
		case QueryPlanAnalyzer::MergeStrategy::CONCATENATE:
			query_info->set_merge_strategy("CONCATENATE");
			break;
		case QueryPlanAnalyzer::MergeStrategy::AGGREGATE_MERGE:
			query_info->set_merge_strategy("AGGREGATE");
			break;
		case QueryPlanAnalyzer::MergeStrategy::GROUP_BY_MERGE:
			query_info->set_merge_strategy("GROUP_BY");
			break;
		case QueryPlanAnalyzer::MergeStrategy::DISTINCT_MERGE:
			query_info->set_merge_strategy("DISTINCT");
			break;
		}

		query_info->set_query_duration_ms(exec_info.query_duration.count());
		query_info->set_num_workers_used(exec_info.num_workers_used);
		query_info->set_num_tasks_generated(exec_info.num_tasks_generated);

		auto time_since_epoch = exec_info.execution_start_time.time_since_epoch();
		auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(time_since_epoch).count();
		query_info->set_execution_start_time_ms(milliseconds);
	}
}

} // namespace duckdb
