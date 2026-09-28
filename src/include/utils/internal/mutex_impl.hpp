#pragma once

#include <mutex>
#include <shared_mutex>

namespace duckdb {
namespace concurrency {

class mutex;
class shared_mutex;

template <typename M>
class lock_guard;
template <typename M>
class unique_lock;
template <typename M>
class shared_lock;

namespace internal {

template <typename T>
struct standard_impl {
	using type = T;
};

template <typename T>
using standard_impl_t = typename standard_impl<T>::type;

template <>
struct standard_impl<::duckdb::concurrency::mutex> {
	using type = std::mutex;
};

template <>
struct standard_impl<::duckdb::concurrency::shared_mutex> {
	using type = std::shared_mutex;
};

template <typename M>
using mutex_impl_t = standard_impl_t<M>;

template <typename M>
struct standard_impl<::duckdb::concurrency::lock_guard<M>> {
	using type = std::lock_guard<mutex_impl_t<M>>;
};

template <typename M>
struct standard_impl<::duckdb::concurrency::unique_lock<M>> {
	using type = std::unique_lock<mutex_impl_t<M>>;
};

template <typename M>
struct standard_impl<::duckdb::concurrency::shared_lock<M>> {
	using type = std::shared_lock<mutex_impl_t<M>>;
};

template <typename L>
using lock_impl_t = standard_impl_t<L>;

} // namespace internal
} // namespace concurrency
} // namespace duckdb
