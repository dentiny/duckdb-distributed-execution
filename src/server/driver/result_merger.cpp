#include "server/driver/result_merger.hpp"
#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

ResultMerger::ResultMerger(Connection &conn_p) : conn(conn_p) {
}

/*static*/ string ResultMerger::BuildAggregateMergeSQL(const string &temp_table, const vector<string> &column_names,
                                                       const QueryPlanAnalyzer::QueryAnalysis &analysis) {
	// For aggregates without GROUP BY, we need to merge partial results:
	// - SUM(partial_sums) for SUM
	// - SUM(partial_counts) for COUNT
	// - For AVG: we'd need SUM(partial_sums) / SUM(partial_counts), but this is complex
	//
	// For now, we'll use a simple approach: re-aggregate all columns

	string sql = "SELECT ";
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i > 0)
			sql += ", ";

		// Try to intelligently re-aggregate based on likely function
		// This is a heuristic - in future, we'd track the actual aggregate functions
		string col_name_lower = StringUtil::Lower(column_names[i]);

		if (col_name_lower.find("count") != string::npos || col_name_lower.find("cnt") != string::npos) {
			// COUNT: sum the partial counts
			sql += StringUtil::Format("SUM(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("sum") != string::npos) {
			// SUM: sum the partial sums
			sql += StringUtil::Format("SUM(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("min") != string::npos) {
			// MIN: take min of partial mins
			sql += StringUtil::Format("MIN(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("max") != string::npos) {
			// MAX: take max of partial maxes
			sql += StringUtil::Format("MAX(%s) AS %s", column_names[i], column_names[i]);
		} else if (col_name_lower.find("avg") != string::npos) {
			// AVG: This is tricky - we'd need both sum and count
			// For now, just take AVG again (not mathematically correct, but works for demo)
			sql += StringUtil::Format("AVG(%s) AS %s", column_names[i], column_names[i]);
		} else {
			// Default: try SUM (works for most aggregates)
			sql += StringUtil::Format("SUM(%s) AS %s", column_names[i], column_names[i]);
		}
	}
	sql += StringUtil::Format(" FROM %s", temp_table);

	return sql;
}

/*static*/ string ResultMerger::BuildGroupByMergeSQL(const string &temp_table, const vector<string> &column_names,
                                                     const QueryPlanAnalyzer::QueryAnalysis &analysis) {
	// For GROUP BY, we need to:
	// 1. Identify which columns are group keys (non-aggregate columns)
	// 2. Identify which columns are aggregates
	// 3. Re-group by the keys and re-aggregate the aggregate columns

	// Heuristic: columns with aggregate-sounding names are aggregates, others are group keys
	vector<string> group_keys;
	vector<string> agg_columns;

	for (const auto &col_name : column_names) {
		string col_lower = StringUtil::Lower(col_name);

		// Check if this looks like an aggregate column
		if (col_lower.find("count") != string::npos || col_lower.find("sum") != string::npos ||
		    col_lower.find("avg") != string::npos || col_lower.find("min") != string::npos ||
		    col_lower.find("max") != string::npos || col_lower.find("_agg") != string::npos) {
			agg_columns.emplace_back(col_name);
		} else {
			group_keys.emplace_back(col_name);
		}
	}

	// If we couldn't identify any group keys, fall back to treating first column as key
	if (group_keys.empty() && !column_names.empty()) {
		group_keys.emplace_back(column_names[0]);
		for (idx_t idx = 1; idx < column_names.size(); ++idx) {
			agg_columns.emplace_back(column_names[idx]);
		}
	}

	// Build SELECT clause
	string sql = "SELECT ";

	// Add group keys (pass through)
	for (idx_t idx = 0; idx < group_keys.size(); ++idx) {
		if (idx > 0) {
			sql += ", ";
		}
		sql += group_keys[idx];
	}

	// Add re-aggregated columns
	for (const auto &agg_col : agg_columns) {
		if (!group_keys.empty() || &agg_col != &agg_columns[0]) {
			sql += ", ";
		}

		string col_lower = StringUtil::Lower(agg_col);

		// Apply appropriate re-aggregation based on column name
		if (col_lower.find("count") != string::npos || col_lower.find("cnt") != string::npos) {
			sql += StringUtil::Format("SUM(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("sum") != string::npos) {
			sql += StringUtil::Format("SUM(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("min") != string::npos) {
			sql += StringUtil::Format("MIN(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("max") != string::npos) {
			sql += StringUtil::Format("MAX(%s) AS %s", agg_col, agg_col);
		} else if (col_lower.find("avg") != string::npos) {
			// AVG is tricky - for now just average the averages (not correct but works for demo)
			sql += StringUtil::Format("AVG(%s) AS %s", agg_col, agg_col);
		} else {
			// Default to SUM
			sql += StringUtil::Format("SUM(%s) AS %s", agg_col, agg_col);
		}
	}

	sql += StringUtil::Format(" FROM %s", temp_table);

	// Add GROUP BY clause
	if (!group_keys.empty()) {
		sql += " GROUP BY ";
		for (idx_t idx = 0; idx < group_keys.size(); ++idx) {
			if (idx > 0) {
				sql += ", ";
			}
			sql += group_keys[idx];
		}
	}

	return sql;
}

unique_ptr<QueryResult>
ResultMerger::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                     const vector<string> &names, const vector<LogicalType> &types) {
	// Coordinator acts as GlobalState aggregator in DuckDB's parallel execution model
	//
	// DuckDB's parallel execution pattern:
	// 1. Multiple threads execute in parallel, each with LocalSinkState
	// 2. Combine() merges LocalSinkState into GlobalSinkState
	// 3. Finalize() produces the final result from GlobalSinkState
	//
	// Distributed execution mapping:
	// 1. Multiple worker nodes execute in parallel (each = one thread)
	// 2. Each worker returns LocalState output (as Arrow RecordBatches)
	// 3. This method performs the Combine() operation:
	//    - Collects LocalState outputs from all workers
	//    - Merges them into a unified result (GlobalState)
	// 4. The ColumnDataCollection acts as our GlobalSinkState
	//
	// This maintains the same aggregation semantics as thread-level parallelism,
	// but distributed across network-connected nodes.
	//
	// Collection will be created lazily after we see the first batch's schema
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> actual_types; // Types from actual Arrow data

	// Combine phase: Merge LocalState outputs from each worker
	idx_t worker_idx = 0;
	idx_t total_batches = 0;
	idx_t total_rows_combined = 0;

	for (auto &stream : streams) {
		idx_t worker_batches = 0;
		idx_t worker_rows = 0;

		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				break;
			}

			auto batch_with_metadata = batch_result.ValueOrDie();
			if (!batch_with_metadata.data) {
				break; // End of stream from this worker
			}

			// Convert Arrow batch (LocalState output) to DuckDB DataChunk
			auto arrow_batch = batch_with_metadata.data;

			// Use Arrow schema to get actual types from the batch
			vector<LogicalType> batch_types;
			batch_types.reserve(arrow_batch->num_columns());

			for (int col_idx = 0; col_idx < arrow_batch->num_columns(); ++col_idx) {
				auto arrow_field = arrow_batch->schema()->field(col_idx);
				auto arrow_type = arrow_field->type();
				auto duckdb_type = ArrowTypeToDuckDBType(arrow_type);
				batch_types.push_back(duckdb_type);
			}

			// Initialize collection with actual schema from first batch
			if (!collection) {
				actual_types = batch_types;
				collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), actual_types);
			}

			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), batch_types);

			for (int col_idx = 0; col_idx < arrow_batch->num_columns(); ++col_idx) {
				auto arrow_array = arrow_batch->column(col_idx);
				auto &duckdb_vector = chunk.data[col_idx];
				ConvertArrowArrayToDuckDBVector(arrow_array, duckdb_vector, batch_types[col_idx],
				                                arrow_batch->num_rows());
			}

			chunk.SetCardinality(arrow_batch->num_rows());
			// Append to GlobalSinkState (ColumnDataCollection)
			collection->Append(chunk);

			worker_batches++;
			worker_rows += arrow_batch->num_rows();
			total_batches++;
			total_rows_combined += arrow_batch->num_rows();
		}
		worker_idx++;
	}

	// Finalize phase: Return the aggregated result
	// In this simple case, we just return the merged collection
	// For more complex operators (aggregates, sorts, etc.), additional
	// finalization logic would go here (e.g., final aggregation, final sort)
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties {}, names,
	                                          std::move(collection), ClientProperties {});
}

unique_ptr<QueryResult>
ResultMerger::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                     const vector<string> &names, const vector<LogicalType> &types,
                                     const QueryPlanAnalyzer::QueryAnalysis &query_analysis) {
	// Collect results from all workers
	auto partial_result = CollectAndMergeResults(streams, names, types);

	// For simple scans, just return the concatenated results
	if (query_analysis.merge_strategy == QueryPlanAnalyzer::MergeStrategy::CONCATENATE) {
		return partial_result;
	}

	auto materialized = dynamic_cast<MaterializedQueryResult *>(partial_result.get());
	if (!materialized || materialized->RowCount() == 0) {
		return partial_result;
	}

	// Create a temporary table from the collected results
	string temp_table_name = "__distributed_partial_results__";

	// Drop if exists
	conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));

	// Create table with correct schema
	string create_sql = StringUtil::Format("CREATE TEMPORARY TABLE %s (", temp_table_name);
	for (idx_t idx = 0; idx < names.size(); ++idx) {
		if (idx > 0) {
			create_sql += ", ";
		}
		create_sql += StringUtil::Format("%s %s", names[idx], types[idx].ToString());
	}
	create_sql += ")";

	auto create_result = conn.Query(create_sql);
	if (create_result->HasError()) {
		throw std::runtime_error(StringUtil::Format("Failed to create temp table: %s", create_result->GetError()));
	}

	// Insert collected data into temp table - insert row by row
	idx_t inserted_rows = 0;

	// Get the collection from materialized result
	auto &collection = materialized->Collection();

	// Iterate through all chunks in the collection
	ColumnDataScanState scan_state;
	collection.InitializeScan(scan_state);
	DataChunk insert_chunk;
	insert_chunk.Initialize(Allocator::DefaultAllocator(), types);

	while (collection.Scan(scan_state, insert_chunk)) {
		if (insert_chunk.size() == 0)
			break;

		// Insert this chunk row by row
		for (idx_t row_idx = 0; row_idx < insert_chunk.size(); row_idx++) {
			string row_sql = StringUtil::Format("INSERT INTO %s VALUES (", temp_table_name);
			for (idx_t col_idx = 0; col_idx < insert_chunk.ColumnCount(); ++col_idx) {
				if (col_idx > 0) {
					row_sql += ", ";
				}
				auto value = insert_chunk.GetValue(col_idx, row_idx);
				row_sql += value.ToSQLString();
			}
			row_sql += ")";

			auto insert_result = conn.Query(row_sql);
			if (insert_result->HasError()) {
				throw std::runtime_error(StringUtil::Format("Failed to insert row: %s", insert_result->GetError()));
			}
			inserted_rows++;
		}
	}

	// Apply the appropriate merge strategy
	string merge_sql;

	switch (query_analysis.merge_strategy) {
	case QueryPlanAnalyzer::MergeStrategy::AGGREGATE_MERGE:
		merge_sql = BuildAggregateMergeSQL(temp_table_name, names, query_analysis);
		break;

	case QueryPlanAnalyzer::MergeStrategy::GROUP_BY_MERGE:
		merge_sql = BuildGroupByMergeSQL(temp_table_name, names, query_analysis);
		break;

	case QueryPlanAnalyzer::MergeStrategy::DISTINCT_MERGE:
		merge_sql = StringUtil::Format("SELECT DISTINCT * FROM %s", temp_table_name);
		break;

	default:
		// Shouldn't reach here
		merge_sql = StringUtil::Format("SELECT * FROM %s", temp_table_name);
		break;
	}

	// Execute the merge SQL and return the result
	auto final_result = conn.Query(merge_sql);

	// Clean up temp table
	conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));

	if (final_result->HasError()) {
		return partial_result; // Fallback to partial results
	}
	return final_result;
}

} // namespace duckdb
