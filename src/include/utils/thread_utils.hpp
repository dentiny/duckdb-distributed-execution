#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Set thread name to current thread; fail silently if an error happens.
void SetThreadName(const string &thread_name);

// Get the number of cores available to the system.
// On linux platform, this function not only gets physical core number for CPU, but also considers available core number
// within kubernetes pod, and container cgroup resource.
int GetCpuCoreCount();

} // namespace duckdb
