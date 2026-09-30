#pragma once

#include <arrow/status.h>

namespace duckdb {
namespace distributed {
class DistributedRequest;
class RegisterClientRequest;
class StorageConfig;
class TransactionRequest;
} // namespace distributed

// Validate the shape and enum values of stateless protocol requests.
arrow::Status ValidateRequest(const distributed::DistributedRequest &request);
arrow::Status ValidateRequest(const distributed::RegisterClientRequest &request);
arrow::Status ValidateRequest(const distributed::StorageConfig &config);
arrow::Status ValidateRequest(const distributed::TransactionRequest &request);

} // namespace duckdb
