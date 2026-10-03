#include "server/startup_sql.hpp"

#include <cstdlib>

namespace duckdb {

namespace {
constexpr const char *STARTUP_SQL_VARIABLE = "DUCKHERDER_STARTUP_SQL";
} // namespace

arrow::Status RunStartupSQL(DuckDB &db) {
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
