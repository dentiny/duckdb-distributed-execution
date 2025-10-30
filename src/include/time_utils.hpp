// Time related utils.

#pragma once

#include "duckdb/common/types/timestamp.hpp"

#include <chrono>
#include <cstdint>
#include <ctime>

namespace duckdb {

inline constexpr uint64_t kMicrosToNanos = 1000ULL;
inline constexpr uint64_t kSecondsToMicros = 1000ULL * 1000ULL;
inline constexpr uint64_t kSecondsToNanos = 1000ULL * 1000ULL * 1000ULL;
inline constexpr uint64_t kMilliToNanos = 1000ULL * 1000ULL;

// Get current timestamp in steady clock since epoch in nanoseconds.
inline int64_t GetSteadyNowNanoSecSinceEpoch() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

// Get current timestamp in steady clock since epoch in milliseconds.
inline int64_t GetSteadyNowMilliSecSinceEpoch() {
	return GetSteadyNowNanoSecSinceEpoch() / kMilliToNanos;
}

// Get current timestamp in steady clock since epoch in nanoseconds.
inline int64_t GetSystemNowNanoSecSinceEpoch() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

// Get current timestamp in steady clock since epoch in milliseconds.
inline int64_t GetSystemNowMilliSecSinceEpoch() {
	return GetSystemNowNanoSecSinceEpoch() / kMilliToNanos;
}

// Convert from duckdb [`timestamp_t`] to [`time_t`].
time_t DuckdbTimestampToTimeT(timestamp_t timestamp);

} // namespace duckdb
