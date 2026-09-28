#pragma once

#include <cstdint>

namespace duckdb {

inline constexpr uint64_t INVALID_TRANSACTION_ID = 0;
inline constexpr uint64_t INVALID_REQUEST_SEQUENCE = 0;
inline constexpr uint64_t INITIAL_TRANSACTION_ID = 1;
inline constexpr uint64_t INITIAL_REQUEST_SEQUENCE = 1;

} // namespace duckdb
