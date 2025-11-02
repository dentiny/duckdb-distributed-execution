#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include <cstdint>

namespace duckdb {

// Protocol definition for distributed execution
// Uses Arrow Flight for efficient data transfer

enum class RequestType : uint8_t {
	EXECUTE_SQL = 0,
	CREATE_TABLE = 1,
	DROP_TABLE = 2,
	INSERT_DATA = 3,
	SCAN_TABLE = 4,
	DELETE_DATA = 5,
	TABLE_EXISTS = 6
};

// Request message structure
struct DistributedRequest {
	RequestType type;
	string sql;
	string table_name;

	// For scans
	uint64_t limit = 0;
	uint64_t offset = 0;

	// For INSERT: Arrow RecordBatch will be sent separately via Flight

	// Serialization helpers
	vector<uint8_t> Serialize() const;
	static DistributedRequest Deserialize(const vector<uint8_t> &data);
};

// Response message structure
struct DistributedResponse {
	bool success;
	string error_message;
	uint64_t rows_affected = 0;

	// For TABLE_EXISTS
	bool exists = false;

	// For scans: Arrow RecordBatch will be returned via Flight

	// Serialization helpers
	vector<uint8_t> Serialize() const;
	static DistributedResponse Deserialize(const vector<uint8_t> &data);
};

} // namespace duckdb
