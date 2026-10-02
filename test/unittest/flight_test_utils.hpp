#pragma once

#include "client/transport/distributed_flight_client.hpp"
#include "server/driver/distributed_flight_server.hpp"

#include <memory>
#include <string>
#include <thread>

namespace duckdb {

inline const std::string SERVER_HOST = "0.0.0.0";
inline constexpr int SERVER_PORT = 18815;
inline const std::string SERVER_URL = "grpc://localhost:18815";

// Flight server shared by every test in the process, started on first use.
class FlightTestServer {
public:
	FlightTestServer();

	DistributedFlightServer &GetServer() {
		return *server;
	}

private:
	std::unique_ptr<DistributedFlightServer> server;
	std::thread server_thread;
};

FlightTestServer &GetTestServer();

// Scan all batches returned by the server and count their rows.
uint64_t CountRows(DistributedFlightClient &client, const string &table_name);

void ExecuteAutocommit(DistributedFlightClient &client, const string &sql);

} // namespace duckdb
