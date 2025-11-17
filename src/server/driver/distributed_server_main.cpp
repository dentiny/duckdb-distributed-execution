/*
 * Distributed Server (Driver Node) Main Entry Point
 *
 * This executable starts a DuckDB distributed execution driver/coordinator node that:
 * - Accepts client connections via Arrow Flight protocol
 * - Manages distributed query execution across worker nodes
 * - Analyzes query plans and creates optimal partition strategies
 * - Coordinates task distribution and result merging
 * - Optionally starts local worker nodes for single-machine distributed execution
 *
 * Usage:
 *   ./distributed_server [host] [port] [num_workers]
 *
 * Arguments:
 *   host         - Host address to bind to (default: 0.0.0.0)
 *   port         - Port to listen on (default: 8815)
 *   num_workers  - Number of local workers to start (default: 0 = no distributed execution)
 *
 * Examples:
 *   ./distributed_server                          # Start on 0.0.0.0:8815 with no workers (local mode)
 *   ./distributed_server 0.0.0.0 8815 4          # Start with 4 local workers
 *   ./distributed_server localhost 9000 8        # Start on localhost:9000 with 8 workers
 */

#include <csignal>
#include <iostream>
#include <memory>

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "server/driver/distributed_flight_server.hpp"

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

		auto LogServerError = [&](const string &message) {
			if (g_server) {
				auto &db_instance = g_server->GetDatabaseInstance();
				DUCKDB_LOG_ERROR(db_instance, message);
			}
		};

		arrow::Status status;
		if (num_workers > 0) {
			status = g_server->StartWithWorkers(num_workers);
		} else {
			status = g_server->Start();
		}

		if (!status.ok()) {
			LogServerError(StringUtil::Format("Failed to start server: %s", status.ToString()));
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
			LogServerError(StringUtil::Format("Server error: %s", serve_status.ToString()));
			return 1;
		}
	} catch (const std::exception &ex) {
		if (g_server) {
			auto &db_instance = g_server->GetDatabaseInstance();
			DUCKDB_LOG_ERROR(db_instance, StringUtil::Format("Fatal error: %s", ex.what()));
		} else {
			std::cout << "Fatal error: " << ex.what() << std::endl;
		}
		return 1;
	}

	return 0;
}
