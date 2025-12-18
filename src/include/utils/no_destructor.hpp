// Copyright 2023 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is adapted from https://github.com/abseil/abseil-cpp/blob/master/absl/base/no_destructor.h
//
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

#include <new>
#include <type_traits>
#include <utility>

namespace duckdb {

template <typename T>
class NoDestructor {
public:
	// Forwards arguments to the T's constructor: calls T(args...).
	template <typename... Ts,
	          // Disable this overload when it might collide with copy/move.
	          typename std::enable_if<
	              !std::is_same<void(typename std::decay<Ts>::type &...), void(NoDestructor &)>::value, int>::type = 0>
	explicit constexpr NoDestructor(Ts &&...args) : impl_(std::forward<Ts>(args)...) {
	}

	// Forwards copy and move construction for T.
	explicit constexpr NoDestructor(const T &x) : impl_(x) {
	}
	explicit constexpr NoDestructor(T &&x) : impl_(std::move(x)) {
	}

	// No copy and move.
	NoDestructor(const NoDestructor &) = delete;
	NoDestructor &operator=(const NoDestructor &) = delete;

	// Pretend to be a smart pointer to T with deep constness.
	// Never returns a null pointer.
	T &operator*() {
		return *get();
	}
	T *operator->() {
		return get();
	}
	T *get() {
		return impl_.get();
	}
	const T &operator*() const {
		return *get();
	}
	const T *operator->() const {
		return get();
	}
	const T *get() const {
		return impl_.get();
	}

private:
	class DirectImpl {
	public:
		template <typename... Args>
		explicit constexpr DirectImpl(Args &&...args) : value_(std::forward<Args>(args)...) {
		}
		const T *get() const {
			return &value_;
		}
		T *get() {
			return &value_;
		}

	private:
		T value_;
	};

	class PlacementImpl {
	public:
		template <typename... Args>
		explicit PlacementImpl(Args &&...args) {
			new (&space_) T(std::forward<Args>(args)...);
		}
		const T *get() const {
			return reinterpret_cast<const T *>(&space_);
		}
		T *get() {
			return reinterpret_cast<T *>(&space_);
		}

	private:
		alignas(T) unsigned char space_[sizeof(T)];
	};

	typename std::conditional<std::is_trivially_destructible<T>::value, DirectImpl, PlacementImpl>::type impl_;
};

} // namespace duckdb
