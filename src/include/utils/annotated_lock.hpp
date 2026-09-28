#pragma once

#include "utils/internal/mutex_impl.hpp"
#include "utils/thread_annotation.hpp"

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace duckdb {
namespace concurrency {

template <typename M>
class DUCKDB_SCOPED_CAPABILITY lock_guard : public internal::lock_impl_t<lock_guard<M>> {
private:
	using Impl = internal::lock_impl_t<lock_guard<M>>;

public:
	explicit lock_guard(M &mutex) DUCKDB_ACQUIRE(mutex) : Impl(mutex) {
	}

	lock_guard(M &mutex, std::adopt_lock_t tag) DUCKDB_REQUIRES(mutex) : Impl(mutex, tag) {
	}

	~lock_guard() DUCKDB_RELEASE() = default;
};

template <typename M>
class DUCKDB_SCOPED_CAPABILITY unique_lock : public internal::lock_impl_t<unique_lock<M>> {
private:
	using Impl = internal::lock_impl_t<unique_lock<M>>;

public:
	unique_lock() = default;
	explicit unique_lock(M &mutex) DUCKDB_ACQUIRE(mutex) : Impl(mutex) {
	}

	unique_lock(M &mutex, std::defer_lock_t tag) noexcept DUCKDB_EXCLUDES(mutex) : Impl(mutex, tag) {
	}

	unique_lock(M &mutex, std::adopt_lock_t tag) DUCKDB_REQUIRES(mutex) : Impl(mutex, tag) {
	}

	unique_lock(M &mutex, std::try_to_lock_t tag) DUCKDB_TRY_ACQUIRE(true, mutex) : Impl(mutex, tag) {
	}

	unique_lock(unique_lock &&) noexcept = default;
	unique_lock &operator=(unique_lock &&) noexcept = default;
	~unique_lock() DUCKDB_RELEASE() = default;

	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}

	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}

	template <typename Rep, typename Period>
	bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout) DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock_for(timeout);
	}

	template <typename Clock, typename Duration>
	bool try_lock_until(const std::chrono::time_point<Clock, Duration> &timeout) DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock_until(timeout);
	}

	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}
};

template <typename M>
class DUCKDB_SCOPED_CAPABILITY shared_lock : public internal::lock_impl_t<shared_lock<M>> {
private:
	using Impl = internal::lock_impl_t<shared_lock<M>>;

public:
	shared_lock() = default;
	explicit shared_lock(M &mutex) DUCKDB_ACQUIRE_SHARED(mutex) : Impl(mutex) {
	}

	shared_lock(M &mutex, std::defer_lock_t tag) noexcept DUCKDB_EXCLUDES(mutex) : Impl(mutex, tag) {
	}

	shared_lock(M &mutex, std::adopt_lock_t tag) DUCKDB_REQUIRES_SHARED(mutex) : Impl(mutex, tag) {
	}

	shared_lock(M &mutex, std::try_to_lock_t tag) DUCKDB_TRY_ACQUIRE_SHARED(true, mutex) : Impl(mutex, tag) {
	}

	shared_lock(shared_lock &&) noexcept = default;
	shared_lock &operator=(shared_lock &&) noexcept = default;
	~shared_lock() DUCKDB_RELEASE() = default;

	void lock() DUCKDB_ACQUIRE_SHARED() {
		Impl::lock();
	}

	bool try_lock() DUCKDB_TRY_ACQUIRE_SHARED(true) {
		return Impl::try_lock();
	}

	template <typename Rep, typename Period>
	bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout) DUCKDB_TRY_ACQUIRE_SHARED(true) {
		return Impl::try_lock_for(timeout);
	}

	template <typename Clock, typename Duration>
	bool try_lock_until(const std::chrono::time_point<Clock, Duration> &timeout) DUCKDB_TRY_ACQUIRE_SHARED(true) {
		return Impl::try_lock_until(timeout);
	}

	void unlock() DUCKDB_RELEASE_SHARED() {
		Impl::unlock();
	}
};

} // namespace concurrency
} // namespace duckdb
