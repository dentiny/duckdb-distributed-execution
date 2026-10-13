#include "server/startup_sql.hpp"

#include "duckdb/main/config.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"

#include <cstdlib>

namespace duckdb {

namespace {
constexpr const char *STARTUP_SQL_VARIABLE = "DUCKHERDER_STARTUP_SQL";
} // namespace

arrow::Status RunStartupSQL(DuckDB &db) {
	// Register before the startup SQL, which runs on the driver and workers alike.
	RegisterJoinPartitioningSetting(DBConfig::GetConfig(*db.instance));
	const char *sql = std::getenv(STARTUP_SQL_VARIABLE);
	if (!sql) {
		return arrow::Status::OK();
	}
	Connection conn(db);
	auto result = conn.Query(sql);
	if (result->HasError()) {
		return arrow::Status::IOError("Startup SQL failed on '", sql, "': ", result->GetError());
	}
	return arrow::Status::OK();
}

} // namespace duckdb
