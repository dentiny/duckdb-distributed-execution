#pragma once

#include <cstdint>

namespace duckdb {

static constexpr uint32_t DUCKHERDER_PROTOCOL_VERSION = 1;
static constexpr uint64_t DUCKHERDER_CAPABILITY_STRUCTURAL_SCAN = 1ULL << 0U;
static constexpr uint64_t DUCKHERDER_CAPABILITY_SESSIONS = 1ULL << 1U;
static constexpr uint64_t DUCKHERDER_REQUIRED_CAPABILITIES =
    DUCKHERDER_CAPABILITY_STRUCTURAL_SCAN | DUCKHERDER_CAPABILITY_SESSIONS;

} // namespace duckdb
