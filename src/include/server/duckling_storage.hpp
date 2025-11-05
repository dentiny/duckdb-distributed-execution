#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class DucklingStorageExtension : public StorageExtension {
public:
	DucklingStorageExtension();
	~DucklingStorageExtension() = default;
};

} // namespace duckdb

