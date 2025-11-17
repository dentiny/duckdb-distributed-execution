// Client implementation to remote execution server.

#pragma once

#include "distributed_flight_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "query_common.hpp"

namespace duckdb {

// Query execution statistics entry.
struct QueryExecutionStatsEntry {
	string sql;
	string execution_mode;
	string merge_strategy;
	int64_t query_duration_ms = 0;
	int64_t num_workers_used = 0;
	int64_t num_tasks_generated = 0;
	int64_t execution_start_time_ms = 0;

	QueryExecutionStatsEntry() = default;

	explicit QueryExecutionStatsEntry(const distributed::QueryExecutionInfo &proto)
	    : sql(proto.sql()), execution_mode(proto.execution_mode()), merge_strategy(proto.merge_strategy()),
	      query_duration_ms(proto.query_duration_ms()), num_workers_used(proto.num_workers_used()),
	      num_tasks_generated(proto.num_tasks_generated()), execution_start_time_ms(proto.execution_start_time_ms()) {
	}
};

class DistributedClient {
public:
	explicit DistributedClient(string server_url_p = "grpc://localhost:8815");
	~DistributedClient() = default;

	static DistributedClient &GetInstance();

	// Execute arbitrary SQL on the server.
	unique_ptr<QueryResult> ExecuteSQL(const string &sql);

	// Check if table exists.
	bool TableExists(const string &table_name);

	// CREATE TABLE on server.
	// Return error status if the table already exists.
	unique_ptr<QueryResult> CreateTable(const string &create_sql);

	// DROP TABLE on server.
	// Return success status if the table doesn't exist.
	unique_ptr<QueryResult> DropTable(const string &drop_sql);

	// CREATE INDEX on server.
	// Return error status if the index already exists.
	unique_ptr<QueryResult> CreateIndex(const string &create_sql);

	// DROP INDEX on server.
	// Return success status if the index doesn't exist.
	unique_ptr<QueryResult> DropIndex(const string &index_name);

	// INSERT INTO on server.
	// TODO(hjiang): Currently for implementation easy, directly execute SQL statements, should be use transfer rows and
	// table name.
	unique_ptr<QueryResult> InsertInto(const string &insert_sql);

	// Get table data.
	// If [`expected_types`] is unassigned, type information is deduced from arrow schema.
	unique_ptr<QueryResult> ScanTable(const string &table_name, idx_t limit = NO_QUERY_LIMIT,
	                                  idx_t offset = NO_QUERY_OFFSET,
	                                  const vector<LogicalType> *expected_types = nullptr);

	// Get query execution statistics from the server.
	// Returns error QueryResult on failure.
	unique_ptr<QueryResult> GetQueryExecutionStats(vector<QueryExecutionStatsEntry> &stats_out);

private:
	string server_url;
	unique_ptr<DistributedFlightClient> client;
};

} // namespace duckdb
