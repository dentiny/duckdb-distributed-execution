#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Set thread name to current thread; fail silently if an error happens.
void SetThreadName(const string &thread_name);

} // namespace duckdb
