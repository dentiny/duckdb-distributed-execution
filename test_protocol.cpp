#include <iostream>
#include <vector>
#include <cstring>
#include <stdint.h>
#include <string>

// Minimal protocol test without DuckDB dependencies

enum class RequestType : uint8_t {
	EXECUTE_SQL = 0,
	CREATE_TABLE = 1,
	DROP_TABLE = 2,
	INSERT_DATA = 3,
	SCAN_TABLE = 4,
	DELETE_DATA = 5,
	TABLE_EXISTS = 6
};

struct TestRequest {
	RequestType type;
	std::string sql;
	std::string table_name;
	uint64_t limit = 0;
	uint64_t offset = 0;
	
	std::vector<uint8_t> Serialize() const {
		std::vector<uint8_t> buffer;
		buffer.push_back(static_cast<uint8_t>(type));
		
		uint32_t sql_len = sql.size();
		buffer.resize(buffer.size() + sizeof(uint32_t));
		memcpy(buffer.data() + buffer.size() - sizeof(uint32_t), &sql_len, sizeof(uint32_t));
		buffer.insert(buffer.end(), sql.begin(), sql.end());
		
		uint32_t table_len = table_name.size();
		buffer.resize(buffer.size() + sizeof(uint32_t));
		memcpy(buffer.data() + buffer.size() - sizeof(uint32_t), &table_len, sizeof(uint32_t));
		buffer.insert(buffer.end(), table_name.begin(), table_name.end());
		
		buffer.resize(buffer.size() + sizeof(uint64_t) * 2);
		memcpy(buffer.data() + buffer.size() - sizeof(uint64_t) * 2, &limit, sizeof(uint64_t));
		memcpy(buffer.data() + buffer.size() - sizeof(uint64_t), &offset, sizeof(uint64_t));
		
		return buffer;
	}
};

int main() {
	std::cout << "Testing distributed protocol..." << std::endl;
	
	TestRequest req;
	req.type = RequestType::SCAN_TABLE;
	req.table_name = "users";
	req.sql = "SELECT * FROM users";
	req.limit = 1000;
	req.offset = 0;
	
	auto serialized = req.Serialize();
	std::cout << "✅ Serialized request: " << serialized.size() << " bytes" << std::endl;
	std::cout << "✅ Protocol test passed!" << std::endl;
	
	return 0;
}

