#pragma once

#include "duckdb/common/types.hpp"

#include <arrow/status.h>
#include <chrono>
#include <functional>

namespace duckdb {

class DatabaseInstance;

inline constexpr const char *RETRY_MAX_ATTEMPTS_SETTING = "duckherder_retry_max_attempts";
inline constexpr const char *RETRY_INITIAL_BACKOFF_MS_SETTING = "duckherder_retry_initial_backoff_ms";
inline constexpr const char *RETRY_MAX_BACKOFF_MS_SETTING = "duckherder_retry_max_backoff_ms";
inline constexpr const char *RETRY_JITTER_RATIO_SETTING = "duckherder_retry_jitter_ratio";

inline constexpr idx_t DEFAULT_RETRY_MAX_ATTEMPTS = 3;
inline constexpr int64_t DEFAULT_RETRY_INITIAL_BACKOFF_MS = 25;
inline constexpr int64_t DEFAULT_RETRY_MAX_BACKOFF_MS = 250;
inline constexpr double DEFAULT_RETRY_JITTER_RATIO = 0.2;

struct RetryConfig {
	idx_t max_attempts = DEFAULT_RETRY_MAX_ATTEMPTS;
	std::chrono::milliseconds initial_delay {DEFAULT_RETRY_INITIAL_BACKOFF_MS};
	std::chrono::milliseconds max_delay {DEFAULT_RETRY_MAX_BACKOFF_MS};
	double jitter_ratio = DEFAULT_RETRY_JITTER_RATIO;
};

// Read the current global retry extension settings from a database instance.
RetryConfig GetRetryConfig(DatabaseInstance &db);

// Execute an operation immediately, then retry transport failures with bounded exponential backoff and jitter.
arrow::Status RetryWithExponentialBackoff(const std::function<arrow::Status()> &operation, const RetryConfig &config);

} // namespace duckdb
