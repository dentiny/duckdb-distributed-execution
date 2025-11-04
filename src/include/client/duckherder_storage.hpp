#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class DuckherderStorageExtension : public StorageExtension {
public:
	DuckherderStorageExtension();
	~DuckherderStorageExtension() = default;
};

} // namespace duckdb
