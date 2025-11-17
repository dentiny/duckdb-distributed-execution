/*
 * Distributed Worker Node Main Entry Point
 *
 * This executable starts a standalone DuckDB worker node that:
 * - Listens for task execution requests from the driver/coordinator node
 * - Executes partitioned queries on its local database instance
 * - Streams results back to the coordinator via Arrow Flight
 * - Operates independently with its own DuckDB instance and storage
 *
 * Usage:
 *   ./distributed_worker [host] [port] [worker_id]
 *
 * Arguments:
 *   host       - Host address to bind to (default: 0.0.0.0)
 *   port       - Port to listen on (default: 8816)
 *   worker_id  - Unique identifier for this worker (default: worker-1)
 *
 * Examples:
 *   ./distributed_worker                                # Start on 0.0.0.0:8816 as worker-1
 *   ./distributed_worker 0.0.0.0 8817 worker-2         # Start on port 8817 as worker-2
 *   ./distributed_worker 192.168.1.10 8816 worker-3    # Start on specific IP as worker-3
 *
 * Note:
 *   - Each worker needs a unique port if running multiple workers on the same machine
 *   - Worker nodes must be manually registered with the coordinator using the
 *     duckherder_register_worker() function or will be auto-discovered if started
 *     via the coordinator's StartWithWorkers() method
 */

#include <csignal>
#include <iostream>
#include <memory>

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "server/worker/worker_node.hpp"

using namespace duckdb;

namespace {
std::unique_ptr<WorkerNode> g_worker;

void SignalHandler(int signal) {
	std::cout << "Received signal " << signal << ", shutting down worker..." << std::endl;
	if (g_worker) {
		g_worker->Shutdown();
	}
	exit(0);
}
} // namespace

int main(int argc, char *argv[]) {
	std::string host = "0.0.0.0";
	int port = 8816;
	std::string worker_id = "worker-1";

	if (argc > 1) {
		host = argv[1];
	}
	if (argc > 2) {
		port = std::stoi(argv[2]);
	}
	if (argc > 3) {
		worker_id = argv[3];
	}

	std::cout << "Starting Distributed Execution Worker Node" << std::endl;
	std::cout << "Worker ID: " << worker_id << std::endl;
	std::cout << "Host: " << host << std::endl;
	std::cout << "Port: " << port << std::endl;

	// Setup signal handlers.
	signal(SIGINT, SignalHandler);
	signal(SIGTERM, SignalHandler);

	try {
		// Create and start worker node.
		// Pass nullptr for shared_db to create an independent database instance.
		g_worker = std::make_unique<WorkerNode>(worker_id, host, port, nullptr);

		auto status = g_worker->Start();
		if (!status.ok()) {
			std::cerr << "Failed to start worker: " << status.ToString() << std::endl;
			return 1;
		}

		std::cout << "Worker node started successfully!" << std::endl;
		std::cout << "Location: " << g_worker->GetLocation() << std::endl;
		std::cout << "Waiting for tasks from coordinator..." << std::endl;
		std::cout << "Press Ctrl+C to stop" << std::endl;

		// Keep worker running.
		auto serve_status = g_worker->Serve();
		if (!serve_status.ok()) {
			std::cerr << "Worker error: " << serve_status.ToString() << std::endl;
			return 1;
		}
	} catch (const std::exception &ex) {
		std::cerr << "Fatal error: " << ex.what() << std::endl;
		return 1;
	}

	return 0;
}
