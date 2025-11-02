// A wrapper class which defines a static type that does not need to be destructed upon program exit. Instead,
// such an object survives during program exit (and can be safely accessed at any time).
//
// In theory, the best implementation is `absl::NoDestructor<T>`.
// Reference: https://github.com/abseil/abseil-cpp/blob/master/absl/base/no_destructor.h
// But C++11 doesn't support `std::launder` so we have to switch `new`-allocation method, instead of `placement new`.
//
// Example usage:
// - Initialization: NoDestructor<T> obj{...};
// - Re-assignment: *obj = T{...};

#pragma once

#include <utility>

namespace duckdb {

template <typename T>
class NoDestructor {
public:
	NoDestructor() : obj(*new T {}) {
	}

	explicit NoDestructor(const T &data) : obj(*new T {data}) {
	}

	explicit NoDestructor(T &&data) : obj(*new T {std::move(data)}) {
	}

	template <typename... Args>
	explicit NoDestructor(Args &&...args) : obj(*new T {std::forward<Args>(args)...}) {
	}

	NoDestructor(const NoDestructor &) = delete;
	NoDestructor &operator=(const NoDestructor &) = delete;
	NoDestructor(NoDestructor &&) = delete;
	NoDestructor &operator=(NoDestructor &&) = delete;

	// Intentionally no destruct.
	~NoDestructor() = default;

	T &Get() & {
		return obj;
	}
	const T &Get() const & {
		return obj;
	}

	T &operator*() & {
		return Get();
	}
	const T &operator*() const & {
		return Get();
	}

	T *operator->() & {
		return &Get();
	}
	const T *operator->() const & {
		return &Get();
	}

private:
	T &obj;
};

} // namespace duckdb
