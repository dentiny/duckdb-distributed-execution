#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Forward declaration.
struct AlterTableInfo;

// Util function to sanitize query and remove all occurrences of the catalog prefix from SQL string.
string SanitizeQuery(const string &sql, const string &catalog_name);

// Generate SQL statement to alter table.
string GenerateAlterTableSQL(AlterTableInfo &info, const string &table_name);

}  // namespace duckdb
