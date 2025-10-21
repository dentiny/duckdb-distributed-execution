#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

class MotherduckTableMetadata {
public:
	MotherduckTableMetadata(const string &schema, const string &table);

	~MotherduckTableMetadata() = default;

private:
	const string &schema;
	const string &table;
};

}; // namespace duckdb
