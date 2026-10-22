#pragma once

#include "duckdb/common/mutex.hpp"

namespace duckdb {
namespace concurrency {

using mutex = annotated_mutex;
using shared_mutex = annotated_shared_mutex;

template <typename M>
using lock_guard = annotated_lock_guard<M>;
template <typename M>
using unique_lock = annotated_unique_lock<M>;
template <typename M>
using shared_lock = annotated_shared_lock<M>;

} // namespace concurrency
} // namespace duckdb
