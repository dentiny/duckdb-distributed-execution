#pragma once

#include "base_query_recorder.hpp"

namespace duckdb {

// Get the global query recorder.
BaseQueryRecorder &GetQueryRecorder();

} // namespace duckdb
