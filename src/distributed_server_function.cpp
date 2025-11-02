#include "distributed_flight_server.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension.hpp"

#include <memory>
#include <thread>

namespace duckdb {

// Global server instance and thread for SQL tests
static std::unique_ptr<DistributedFlightServer> g_test_server;
static std::unique_ptr<std::thread> g_server_thread;
static bool g_server_started = false;
static std::mutex g_server_mutex;

static void StartDistributedServerScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Lock to ensure only one server starts
	std::lock_guard<std::mutex> lock(g_server_mutex);

	if (g_server_started) {
		// Server already running
		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Server already running");
		return;
	}

	// Get port from argument (default 8815)
	int port = 8815;
	if (args.ColumnCount() > 0 && args.size() > 0) {
		auto &port_vector = args.data[0];
		auto port_data = FlatVector::GetData<int32_t>(port_vector);
		port = port_data[0];
	}

	try {
		// Create server
		g_test_server = std::make_unique<DistributedFlightServer>("0.0.0.0", port);

		auto status = g_test_server->Start();
		if (!status.ok()) {
			throw Exception(ExceptionType::IO, "Failed to start server: " + status.ToString());
		}

		// Start server in background thread (detached to avoid cleanup issues)
		g_server_thread = std::make_unique<std::thread>([port]() {
			// This thread owns its own server instance
			auto serve_status = g_test_server->Serve();
			if (!serve_status.ok() && g_server_started) {
				std::cerr << "Server error on port " << port << ": " << serve_status.ToString() << std::endl;
			}
		});

		// Detach thread to avoid cleanup issues
		if (g_server_thread->joinable()) {
			g_server_thread->detach();
		}

		// Give server time to initialize
		std::this_thread::sleep_for(std::chrono::milliseconds(500));

		g_server_started = true;

		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Distributed server started on port " + std::to_string(port));

		// Register cleanup handler
		static bool cleanup_registered = false;
		if (!cleanup_registered) {
			std::atexit([]() {
				if (g_test_server) {
					g_test_server->Shutdown();
				}
			});
			cleanup_registered = true;
		}

	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start distributed server: " + string(ex.what()));
	}
}

static void StopDistributedServerScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	std::lock_guard<std::mutex> lock(g_server_mutex);

	if (!g_server_started) {
		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Server not running");
		return;
	}

	if (g_test_server) {
		g_test_server->Shutdown();
	}

	if (g_server_thread && g_server_thread->joinable()) {
		g_server_thread->join();
	}

	g_test_server.reset();
	g_server_thread.reset();
	g_server_started = false;

	auto result_data = FlatVector::GetData<string_t>(result);
	result_data[0] = StringVector::AddString(result, "Distributed server stopped");
}

void RegisterDistributedServerFunctions(ExtensionLoader &loader) {
	// Register start_distributed_server() function
	auto start_func =
	    ScalarFunction("start_distributed_server", {}, LogicalType::VARCHAR, StartDistributedServerScalarFunction);
	loader.RegisterFunction(start_func);

	// Overload with port parameter
	auto start_func_with_port = ScalarFunction("start_distributed_server", {LogicalType::INTEGER}, LogicalType::VARCHAR,
	                                           StartDistributedServerScalarFunction);
	loader.RegisterFunction(start_func_with_port);

	// Register stop_distributed_server() function
	auto stop_func =
	    ScalarFunction("stop_distributed_server", {}, LogicalType::VARCHAR, StopDistributedServerScalarFunction);
	loader.RegisterFunction(stop_func);
}

} // namespace duckdb
