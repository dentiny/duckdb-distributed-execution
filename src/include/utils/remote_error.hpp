#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "error.pb.h"

namespace duckdb {

// Converts a DuckDB exception type to its stable wire representation.
distributed::RemoteExceptionType ToRemoteExceptionType(ExceptionType type);

// Converts a validated wire exception type back to DuckDB's representation.
ExceptionType FromRemoteExceptionType(distributed::RemoteExceptionType type);

// Serializes a DuckDB application error into its protobuf representation.
void ToRemoteError(const ErrorData &error, distributed::RemoteError &remote_error);

// Reconstructs a DuckDB application error from its protobuf representation.
ErrorData FromRemoteError(const distributed::RemoteError &remote_error);

} // namespace duckdb
