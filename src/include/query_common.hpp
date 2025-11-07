#pragma once

#include <limits>

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

// Default duery limit, which enforce no limit on queries.
inline constexpr idx_t NO_QUERY_LIMIT = std::numeric_limits<idx_t>::max();
// Default query offset, which applies no offset.
inline constexpr idx_t NO_QUERY_OFFSET = 0;

} // namespace duckdb
