#include "distributed_protocol.hpp"
#include <cstring>

namespace duckdb {

vector<uint8_t> DistributedRequest::Serialize() const {
	vector<uint8_t> buffer;

	// Type (1 byte)
	buffer.push_back(static_cast<uint8_t>(type));

	// SQL length + SQL
	uint32_t sql_len = sql.size();
	buffer.resize(buffer.size() + sizeof(uint32_t));
	memcpy(buffer.data() + buffer.size() - sizeof(uint32_t), &sql_len, sizeof(uint32_t));
	buffer.insert(buffer.end(), sql.begin(), sql.end());

	// Table name length + table name
	uint32_t table_len = table_name.size();
	buffer.resize(buffer.size() + sizeof(uint32_t));
	memcpy(buffer.data() + buffer.size() - sizeof(uint32_t), &table_len, sizeof(uint32_t));
	buffer.insert(buffer.end(), table_name.begin(), table_name.end());

	// Limit and offset
	buffer.resize(buffer.size() + sizeof(uint64_t) * 2);
	memcpy(buffer.data() + buffer.size() - sizeof(uint64_t) * 2, &limit, sizeof(uint64_t));
	memcpy(buffer.data() + buffer.size() - sizeof(uint64_t), &offset, sizeof(uint64_t));

	return buffer;
}

DistributedRequest DistributedRequest::Deserialize(const vector<uint8_t> &data) {
	DistributedRequest req;
	size_t pos = 0;

	// Type
	req.type = static_cast<RequestType>(data[pos++]);

	// SQL
	uint32_t sql_len;
	memcpy(&sql_len, data.data() + pos, sizeof(uint32_t));
	pos += sizeof(uint32_t);
	req.sql = string(reinterpret_cast<const char *>(data.data() + pos), sql_len);
	pos += sql_len;

	// Table name
	uint32_t table_len;
	memcpy(&table_len, data.data() + pos, sizeof(uint32_t));
	pos += sizeof(uint32_t);
	req.table_name = string(reinterpret_cast<const char *>(data.data() + pos), table_len);
	pos += table_len;

	// Limit and offset
	memcpy(&req.limit, data.data() + pos, sizeof(uint64_t));
	pos += sizeof(uint64_t);
	memcpy(&req.offset, data.data() + pos, sizeof(uint64_t));

	return req;
}

vector<uint8_t> DistributedResponse::Serialize() const {
	vector<uint8_t> buffer;

	// Success (1 byte)
	buffer.push_back(success ? 1 : 0);

	// Error message length + error message
	uint32_t error_len = error_message.size();
	buffer.resize(buffer.size() + sizeof(uint32_t));
	memcpy(buffer.data() + buffer.size() - sizeof(uint32_t), &error_len, sizeof(uint32_t));
	buffer.insert(buffer.end(), error_message.begin(), error_message.end());

	// Rows affected
	buffer.resize(buffer.size() + sizeof(uint64_t));
	memcpy(buffer.data() + buffer.size() - sizeof(uint64_t), &rows_affected, sizeof(uint64_t));

	// Exists flag
	buffer.push_back(exists ? 1 : 0);

	return buffer;
}

DistributedResponse DistributedResponse::Deserialize(const vector<uint8_t> &data) {
	DistributedResponse resp;
	size_t pos = 0;

	// Success
	resp.success = data[pos++] != 0;

	// Error message
	uint32_t error_len;
	memcpy(&error_len, data.data() + pos, sizeof(uint32_t));
	pos += sizeof(uint32_t);
	resp.error_message = string(reinterpret_cast<const char *>(data.data() + pos), error_len);
	pos += error_len;

	// Rows affected
	memcpy(&resp.rows_affected, data.data() + pos, sizeof(uint64_t));
	pos += sizeof(uint64_t);

	// Exists
	resp.exists = data[pos] != 0;

	return resp;
}

} // namespace duckdb
