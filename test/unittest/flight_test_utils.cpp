#include "flight_test_utils.hpp"

#include "catch/catch.hpp"
#include "utils/no_destructor.hpp"

#include <chrono>
#include <iostream>

namespace duckdb {

FlightTestServer::FlightTestServer() : server(std::make_unique<DistributedFlightServer>(SERVER_HOST, SERVER_PORT)) {
	auto status = server->Start();
	if (!status.ok()) {
		throw std::runtime_error("Failed to start server: " + status.ToString());
	}
	server_thread = std::thread([this] {
		auto serve_status = server->Serve();
		if (!serve_status.ok()) {
			std::cerr << "Server error: " << serve_status.ToString() << std::endl;
		}
	});
	std::this_thread::sleep_for(std::chrono::seconds(2));
}

FlightTestServer &GetTestServer() {
	static NoDestructor<FlightTestServer> test_server;
	return *test_server;
}

uint64_t CountRows(DistributedFlightClient &client, const string &table_name) {
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	REQUIRE(client.ScanTable(table_name, 100, 0, batches).ok());
	uint64_t row_count = 0;
	for (const auto &batch : batches) {
		row_count += batch->num_rows();
	}
	return row_count;
}

void ExecuteAutocommit(DistributedFlightClient &client, const string &sql) {
	distributed::DistributedResponse response;
	REQUIRE(client.ExecuteStatement(sql, "", response).ok());
	REQUIRE(response.success());
}

} // namespace duckdb
