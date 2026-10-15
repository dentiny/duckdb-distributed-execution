#include "utils/thread_utils.hpp"

#include <pthread.h>

namespace duckdb {

void SetThreadName(const string &thread_name) {
#if defined(__APPLE__)
	pthread_setname_np(thread_name.c_str());
#elif defined(__linux__)
	// Restricted to 16 characters, include terminator.
	pthread_setname_np(pthread_self(), thread_name.substr(0, 15).c_str());
#endif
}

} // namespace duckdb
