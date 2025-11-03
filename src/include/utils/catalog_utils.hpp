#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Util function to sanitize query and remove all occurrences of the catalog prefix from SQL string.
string SanitizeQuery(const string &sql, const string &catalog_name);

}  // namespace duckdb
