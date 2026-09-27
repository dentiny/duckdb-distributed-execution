#include "utils/retry_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/database.hpp"

#include <limits>
#include <thread>

namespace duckdb {

namespace {

Value GetRetrySetting(DatabaseInstance &db, const string &name) {
	Value value;
	if (!db.TryGetCurrentSetting(name, value)) {
		throw InternalException("Duckherder retry setting %s is not registered", name);
	}
	return value;
}

bool IsRetryableTransportError(const arrow::Status &status) {
	return status.IsIOError() || status.IsUnknownError() || status.IsCancelled();
}

} // namespace

RetryConfig GetDefaultRetryConfig() {
	return RetryConfig {DEFAULT_RETRY_MAX_ATTEMPTS, std::chrono::milliseconds(DEFAULT_RETRY_INITIAL_BACKOFF_MS),
	                    std::chrono::milliseconds(DEFAULT_RETRY_MAX_BACKOFF_MS), DEFAULT_RETRY_JITTER_RATIO};
}

RetryConfig GetRetryConfig(DatabaseInstance &db) {
	return RetryConfig {
	    GetRetrySetting(db, RETRY_MAX_ATTEMPTS_SETTING).GetValue<idx_t>(),
	    std::chrono::milliseconds(GetRetrySetting(db, RETRY_INITIAL_BACKOFF_MS_SETTING).GetValue<int64_t>()),
	    std::chrono::milliseconds(GetRetrySetting(db, RETRY_MAX_BACKOFF_MS_SETTING).GetValue<int64_t>()),
	    GetRetrySetting(db, RETRY_JITTER_RATIO_SETTING).GetValue<double>()};
}

arrow::Status RetryWithExponentialBackoff(const std::function<arrow::Status()> &operation, const RetryConfig &config) {
	if (config.max_attempts == 0) {
		return arrow::Status::Invalid("Retry max_attempts must be greater than zero");
	}
	if (config.initial_delay.count() < 0 || config.max_delay.count() < 0 || config.initial_delay > config.max_delay) {
		return arrow::Status::Invalid("Retry delays must be non-negative and initial_delay must not exceed max_delay");
	}
	if (config.jitter_ratio < 0.0 || config.jitter_ratio > 1.0) {
		return arrow::Status::Invalid("Retry jitter_ratio must be between zero and one");
	}

	RandomEngine random;
	auto delay = config.initial_delay;
	for (idx_t attempt = 0; attempt < config.max_attempts; attempt++) {
		auto status = operation();
		if (status.ok() || !IsRetryableTransportError(status) || attempt + 1 == config.max_attempts) {
			return status;
		}

		auto jitter = random.NextRandom(1.0 - config.jitter_ratio, 1.0 + config.jitter_ratio);
		auto jittered_delay_count = static_cast<long double>(delay.count()) * jitter;
		auto maximum_delay_count = static_cast<long double>(std::numeric_limits<int64_t>::max());
		auto jittered_delay = std::chrono::milliseconds(jittered_delay_count >= maximum_delay_count
		                                                    ? std::numeric_limits<int64_t>::max()
		                                                    : static_cast<int64_t>(jittered_delay_count));
		std::this_thread::sleep_for(jittered_delay);

		auto next_delay = delay.count() > config.max_delay.count() / 2
		                      ? config.max_delay.count()
		                      : MinValue<int64_t>(delay.count() * 2, config.max_delay.count());
		delay = std::chrono::milliseconds(next_delay);
	}
	return arrow::Status::Invalid("Retry loop terminated without an attempt");
}

} // namespace duckdb
