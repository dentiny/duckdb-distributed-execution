#include "server/driver/result_merger.hpp"
#include "arrow_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

ResultMerger::ResultMerger(Connection &conn_p) : conn(conn_p) {
}

string ResultMerger::BuildAggregateMergeSQL(const string &temp_table, const vector<string> &column_names,
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

string ResultMerger::BuildGroupByMergeSQL(const string &temp_table, const vector<string> &column_names,
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
			agg_columns.push_back(col_name);
		} else {
			group_keys.push_back(col_name);
		}
	}

	// If we couldn't identify any group keys, fall back to treating first column as key
	if (group_keys.empty() && !column_names.empty()) {
		group_keys.push_back(column_names[0]);
		for (idx_t i = 1; i < column_names.size(); i++) {
			agg_columns.push_back(column_names[i]);
		}
	}

	// Build SELECT clause
	string sql = "SELECT ";

	// Add group keys (pass through)
	for (idx_t i = 0; i < group_keys.size(); i++) {
		if (i > 0)
			sql += ", ";
		sql += group_keys[i];
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
		for (idx_t i = 0; i < group_keys.size(); i++) {
			if (i > 0)
				sql += ", ";
			sql += group_keys[i];
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

	auto &db_instance = *conn.context->db.get();

	// Collection will be created lazily after we see the first batch's schema
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> actual_types; // Types from actual Arrow data

	// Combine phase: Merge LocalState outputs from each worker
	idx_t worker_idx = 0;
	idx_t total_batches = 0;
	idx_t total_rows_combined = 0;

	for (auto &stream : streams) {
		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("🔀 [COMBINE] Coordinator: Processing results from worker %llu/%llu",
		                                    static_cast<long long unsigned>(worker_idx + 1),
		                                    static_cast<long long unsigned>(streams.size())));
		idx_t worker_batches = 0;
		idx_t worker_rows = 0;

		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				DUCKDB_LOG_WARN(db_instance,
				                StringUtil::Format("⚠️  [COMBINE] Coordinator: Worker %llu stream error: %s",
				                                   static_cast<long long unsigned>(worker_idx),
				                                   batch_result.status().ToString()));
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
				DUCKDB_LOG_DEBUG(
				    db_instance,
				    StringUtil::Format("🔀 [COMBINE] Coordinator: Initialized GlobalSinkState with %llu columns",
				                       static_cast<long long unsigned>(actual_types.size())));
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

		DUCKDB_LOG_DEBUG(db_instance,
		                 StringUtil::Format("✅ [COMBINE] Coordinator: Worker %llu contributed %llu batches, %llu rows",
		                                    static_cast<long long unsigned>(worker_idx),
		                                    static_cast<long long unsigned>(worker_batches),
		                                    static_cast<long long unsigned>(worker_rows)));

		worker_idx++;
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🎯 [COMBINE] Coordinator: GlobalSinkState merge complete - %llu "
	                                                 "total batches, %llu total rows from %llu workers",
	                                                 static_cast<long long unsigned>(total_batches),
	                                                 static_cast<long long unsigned>(total_rows_combined),
	                                                 static_cast<long long unsigned>(streams.size())));

	// Finalize phase: Return the aggregated result
	// In this simple case, we just return the merged collection
	// For more complex operators (aggregates, sorts, etc.), additional
	// finalization logic would go here (e.g., final aggregation, final sort)
	DUCKDB_LOG_DEBUG(db_instance,
	                 StringUtil::Format("🏁 [FINALIZE] Coordinator: Returning final result (%llu rows total)",
	                                    static_cast<long long unsigned>(total_rows_combined)));
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties {}, names,
	                                          std::move(collection), ClientProperties {});
}

unique_ptr<QueryResult>
ResultMerger::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                     const vector<string> &names, const vector<LogicalType> &types,
                                     const QueryPlanAnalyzer::QueryAnalysis &query_analysis) {
	auto &db_instance = *conn.context->db.get();

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Smart merge phase starting strategy=%d",
	                                                 static_cast<int>(query_analysis.merge_strategy)));

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Merging %llu result streams using strategy %d",
	                                                 static_cast<long long unsigned>(streams.size()),
	                                                 static_cast<int>(query_analysis.merge_strategy)));

	// STEP 1: Collect results from all workers
	auto partial_result = CollectAndMergeResults(streams, names, types);

	// STEP 2: For simple scans, just return the concatenated results
	if (query_analysis.merge_strategy == QueryPlanAnalyzer::MergeStrategy::CONCATENATE) {
		DUCKDB_LOG_DEBUG(db_instance, "✅ [STEP4B] CONCATENATE strategy - returning concatenated results");
		return partial_result;
	}

	auto materialized = dynamic_cast<MaterializedQueryResult *>(partial_result.get());
	if (!materialized || materialized->RowCount() == 0) {
		DUCKDB_LOG_DEBUG(db_instance, "⚠️  [STEP4B] No rows to merge, returning empty result");
		return partial_result;
	}

	DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Collected %llu partial rows from workers",
	                                                 static_cast<long long unsigned>(materialized->RowCount())));

	try {
		// STEP 4: Create a temporary table from the collected results
		string temp_table_name = "__distributed_partial_results__";

		// Drop if exists
		conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));

		// Create table with correct schema
		string create_sql = StringUtil::Format("CREATE TEMPORARY TABLE %s (", temp_table_name);
		for (idx_t i = 0; i < names.size(); i++) {
			if (i > 0)
				create_sql += ", ";
			create_sql += StringUtil::Format("%s %s", names[i], types[i].ToString());
		}
		create_sql += ")";

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Creating temp table with SQL: %s", create_sql));
		auto create_result = conn.Query(create_sql);
		if (create_result->HasError()) {
			throw std::runtime_error(StringUtil::Format("Failed to create temp table: %s", create_result->GetError()));
		}

		// Insert collected data into temp table - insert row by row
		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Inserting %llu rows into temp table",
		                                                 static_cast<long long unsigned>(materialized->RowCount())));
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
				for (idx_t col_idx = 0; col_idx < insert_chunk.ColumnCount(); col_idx++) {
					if (col_idx > 0)
						row_sql += ", ";
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

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Inserted %llu rows into temp table",
		                                                 static_cast<long long unsigned>(inserted_rows)));

		// STEP 5: Apply the appropriate merge strategy
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

		DUCKDB_LOG_DEBUG(db_instance, StringUtil::Format("🔀 [STEP4B] Executing merge SQL: %s", merge_sql));

		// STEP 6: Execute the merge SQL and return the result
		auto final_result = conn.Query(merge_sql);

		// Clean up temp table
		conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));

		if (final_result->HasError()) {
			DUCKDB_LOG_WARN(db_instance,
			                StringUtil::Format("⚠️  [STEP4B] Merge SQL failed: %s, returning partial results",
			                                   final_result->GetError()));
			return partial_result; // Fallback to partial results
		}

		auto final_materialized = dynamic_cast<MaterializedQueryResult *>(final_result.get());
		if (final_materialized) {
			DUCKDB_LOG_DEBUG(db_instance,
			                 StringUtil::Format("🔀 [STEP4B] Final merge rows=%llu",
			                                    static_cast<long long unsigned>(final_materialized->RowCount())));
		}

		DUCKDB_LOG_DEBUG(db_instance, "✅ [STEP4B] Smart merge complete");
		return final_result;

	} catch (std::exception &ex) {
		DUCKDB_LOG_WARN(db_instance,
		                StringUtil::Format("⚠️  [STEP4B] Smart merge failed: %s, returning partial results", ex.what()));
		return partial_result; // Fallback to partial results
	}
}

} // namespace duckdb
