#include "distributed_flight_server.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <thread>

namespace duckdb {

namespace {

// Global server instance and thread.
unique_ptr<DistributedFlightServer> g_test_server;
bool g_server_started = false;
std::mutex g_server_mutex;
constexpr int DEFAULT_SERVER_PORT = 8815;

void StartLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);
	
	std::cerr << "[StartLocalServer] Called, g_server_started=" << g_server_started << std::endl;

	if (g_server_started) {
		std::cerr << "[StartLocalServer] Server already running, returning" << std::endl;
		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Server already running");
		return;
	}

	int port = DEFAULT_SERVER_PORT;
	if (args.ColumnCount() > 0 && args.size() > 0) {
		auto &port_vector = args.data[0];
		auto port_data = FlatVector::GetData<int32_t>(port_vector);
		port = port_data[0];
	}
	
	std::cerr << "[StartLocalServer] Starting server on port " << port << std::endl;

	try {
		g_test_server = make_uniq<DistributedFlightServer>("0.0.0.0", port);
		std::cerr << "[StartLocalServer] Created DistributedFlightServer, calling Start()..." << std::endl;
		auto status = g_test_server->Start();
		if (!status.ok()) {
			std::cerr << "[StartLocalServer] Start() failed: " << status.ToString() << std::endl;
			throw Exception(ExceptionType::IO, "Failed to start local server: " + status.ToString());
		}
		std::cerr << "[StartLocalServer] Start() succeeded, now starting Serve() in background thread" << std::endl;

		// Start server in background thread and detach.
		std::thread([port]() {
			std::cerr << "[StartLocalServer background thread] Calling Serve() on port " << port << std::endl;
			// This thread owns its own server instance
			auto serve_status = g_test_server->Serve();
			if (!serve_status.ok() && g_server_started) {
				std::cerr << "Server error on port " << port << ": " << serve_status.ToString() << std::endl;
			}
			std::cerr << "[StartLocalServer background thread] Serve() returned" << std::endl;
		}).detach();

		// TODO(hjiang): Use readiness probe to validate server on.
		std::this_thread::sleep_for(std::chrono::milliseconds(500));

		g_server_started = true;

		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Local server started on port " + std::to_string(port));

		// Note: Cleanup happens via detached thread or explicit duckherder_stop_local_server()
	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start local server: " + string(ex.what()));
	}
}

void StopLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	if (!g_server_started) {
		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Server not running");
		return;
	}

	if (g_test_server) {
		g_test_server->Shutdown();
	}

	g_test_server.reset();
	g_server_started = false;

	auto result_data = FlatVector::GetData<string_t>(result);
	result_data[0] = StringVector::AddString(result, "Local server stopped");
}

} // namespace

ScalarFunction GetStartLocalServerFunction() {
	auto start_func = ScalarFunction("duckherder_start_local_server",
	                                 /*arguments*/ {LogicalType {LogicalTypeId::INTEGER}},
	                                 /*return_type=*/LogicalType {LogicalTypeId::VARCHAR}, StartLocalServer);
	start_func.varargs = LogicalType::ANY;
	return start_func;
}

ScalarFunction GetStopLocalServerFunction() {
	return ScalarFunction("duckherder_stop_local_server",
	                      /*arguments*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::VARCHAR}, StopLocalServer);
}

} // namespace duckdb
