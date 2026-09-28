#pragma once

#include "utils/internal/mutex_impl.hpp"
#include "utils/thread_annotation.hpp"

namespace duckdb {
namespace concurrency {

class DUCKDB_CAPABILITY("mutex") mutex : public internal::mutex_impl_t<mutex> {
private:
	using Impl = internal::mutex_impl_t<mutex>;

public:
	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}

	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}

	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}
};

class DUCKDB_CAPABILITY("mutex") shared_mutex : public internal::mutex_impl_t<shared_mutex> {
private:
	using Impl = internal::mutex_impl_t<shared_mutex>;

public:
	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}

	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}

	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}

	void lock_shared() DUCKDB_ACQUIRE_SHARED() {
		Impl::lock_shared();
	}

	void unlock_shared() DUCKDB_RELEASE_SHARED() {
		Impl::unlock_shared();
	}

	bool try_lock_shared() DUCKDB_TRY_ACQUIRE_SHARED(true) {
		return Impl::try_lock_shared();
	}
};

} // namespace concurrency
} // namespace duckdb
