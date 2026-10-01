#include "utils/remote_error.hpp"

namespace duckdb {

distributed::RemoteExceptionType ToRemoteExceptionType(ExceptionType type) {
	auto value = static_cast<int>(type);
	if (value < 0 || value > static_cast<int>(ExceptionType::INVALID_CONFIGURATION)) {
		throw InternalException("Unsupported DuckDB exception type: %d", value);
	}
	return static_cast<distributed::RemoteExceptionType>(value);
}

ExceptionType FromRemoteExceptionType(distributed::RemoteExceptionType type) {
	auto value = static_cast<int>(type);
	if (value < 0 || value > static_cast<int>(ExceptionType::INVALID_CONFIGURATION)) {
		throw SerializationException("Unsupported remote exception type: %d", value);
	}
	return static_cast<ExceptionType>(value);
}

void ToRemoteError(const ErrorData &error, distributed::RemoteError &remote_error) {
	remote_error.set_exception_type(ToRemoteExceptionType(error.Type()));
	remote_error.set_message(error.RawMessage());
	for (auto &entry : error.ExtraInfo()) {
		remote_error.mutable_extra_info()->emplace(entry.first, entry.second);
	}
}

ErrorData FromRemoteError(const distributed::RemoteError &remote_error) {
	unordered_map<string, string> extra_info;
	for (auto &entry : remote_error.extra_info()) {
		extra_info.emplace(entry.first, entry.second);
	}
	auto type = FromRemoteExceptionType(remote_error.exception_type());
	// An internal or fatal error invalidates the server's database, not the local one, which DuckDB would also
	// invalidate when the error passes through local execution.
	if (type == ExceptionType::INTERNAL || Exception::InvalidatesDatabase(type)) {
		return ErrorData(IOException(extra_info, "Remote %s Error: %s", Exception::ExceptionTypeToString(type),
		                             remote_error.message()));
	}
	return ErrorData(Exception(extra_info, type, remote_error.message()));
}

} // namespace duckdb
