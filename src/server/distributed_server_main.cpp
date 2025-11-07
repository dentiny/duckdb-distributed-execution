#include <csignal>
#include <iostream>
#include <memory>

#include "server/distributed_flight_server.hpp"

using namespace duckdb;

namespace {
std::unique_ptr<DistributedFlightServer> g_server;

void SignalHandler(int signal) {
	std::cout << "Received signal " << signal << ", shutting down..." << std::endl;
	if (g_server) {
		g_server->Shutdown();
	}
	exit(0);
}
} // namespace

int main(int argc, char *argv[]) {
	std::string host = "0.0.0.0";
	int port = 8815;
	int num_workers = 0; // 0 = no distributed execution, run locally

	if (argc > 1) {
		host = argv[1];
	}
	if (argc > 2) {
		port = std::stoi(argv[2]);
	}
	if (argc > 3) {
		num_workers = std::stoi(argv[3]);
	}

	std::cout << "Starting Distributed Execution Server" << std::endl;
	std::cout << "Host: " << host << std::endl;
	std::cout << "Port: " << port << std::endl;
	std::cout << "Workers: " << num_workers << std::endl;

	// Setup signal handlers.
	signal(SIGINT, SignalHandler);
	signal(SIGTERM, SignalHandler);

	try {
		// Create and start Flight server.
		g_server = std::make_unique<DistributedFlightServer>(host, port);

		arrow::Status status;
		if (num_workers > 0) {
			status = g_server->StartWithWorkers(num_workers);
		} else {
			status = g_server->Start();
		}

		if (!status.ok()) {
			std::cerr << "Failed to start server: " << status.ToString() << std::endl;
			return 1;
		}

		std::cout << "Server started successfully!" << std::endl;
		if (num_workers > 0) {
			std::cout << "Distributed mode with " << num_workers << " workers" << std::endl;
		} else {
			std::cout << "Local execution mode (no workers)" << std::endl;
		}
		std::cout << "Waiting for client connections..." << std::endl;
		std::cout << "Press Ctrl+C to stop" << std::endl;

		// Keep server running.
		auto serve_status = g_server->Serve();
		if (!serve_status.ok()) {
			std::cerr << "Server error: " << serve_status.ToString() << std::endl;
			return 1;
		}
	} catch (const std::exception &ex) {
		std::cerr << "Fatal error: " << ex.what() << std::endl;
		return 1;
	}

	return 0;
}
