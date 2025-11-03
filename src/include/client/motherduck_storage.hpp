#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class MotherduckStorageExtension : public StorageExtension {
public:
	MotherduckStorageExtension();
	~MotherduckStorageExtension() = default;
};

} // namespace duckdb
