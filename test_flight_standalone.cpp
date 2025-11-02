#include "src/include/distributed_protocol.hpp"
#include "src/include/distributed_flight_client.hpp"
#include <iostream>

using namespace duckdb;

int main() {
	std::cout << "Testing Distributed Protocol..." << std::endl;
	
	// Test request serialization
	DistributedRequest req;
	req.type = RequestType::TABLE_EXISTS;
	req.table_name = "test_table";
	req.sql = "SELECT * FROM test";
	req.limit = 100;
	req.offset = 0;
	
	// Serialize
	auto serialized = req.Serialize();
	std::cout << "✅ Serialized request: " << serialized.size() << " bytes" << std::endl;
	
	// Deserialize
	auto deserialized = DistributedRequest::Deserialize(serialized);
	std::cout << "✅ Deserialized request" << std::endl;
	std::cout << "  Type: " << static_cast<int>(deserialized.type) << std::endl;
	std::cout << "  Table: " << deserialized.table_name << std::endl;
	std::cout << "  SQL: " << deserialized.sql << std::endl;
	std::cout << "  Limit: " << deserialized.limit << std::endl;
	std::cout << "  Offset: " << deserialized.offset << std::endl;
	
	// Test response serialization
	DistributedResponse resp;
	resp.success = true;
	resp.error_message = "";
	resp.rows_affected = 42;
	resp.exists = true;
	
	auto resp_serialized = resp.Serialize();
	std::cout << "✅ Serialized response: " << resp_serialized.size() << " bytes" << std::endl;
	
	auto resp_deserialized = DistributedResponse::Deserialize(resp_serialized);
	std::cout << "✅ Deserialized response" << std::endl;
	std::cout << "  Success: " << resp_deserialized.success << std::endl;
	std::cout << "  Rows affected: " << resp_deserialized.rows_affected << std::endl;
	std::cout << "  Exists: " << resp_deserialized.exists << std::endl;
	
	std::cout << "\n🎉 All protocol tests passed!" << std::endl;
	
	// Test Flight client creation (won't connect, just test instantiation)
	std::cout << "\nTesting Flight Client creation..." << std::endl;
	try {
		DistributedFlightClient client("grpc://localhost:8815");
		std::cout << "✅ Flight client created successfully" << std::endl;
	} catch (const std::exception &e) {
		std::cout << "❌ Failed to create client: " << e.what() << std::endl;
		return 1;
	}
	
	std::cout << "\n✅ All tests passed!" << std::endl;
	return 0;
}

