#include "distributed_flight_server.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <thread>

namespace duckdb {

namespace {

// Global server instance and thread.
unique_ptr<DistributedFlightServer> g_test_server;
unique_ptr<std::thread> g_server_thread;
bool g_server_started = false;
std::mutex g_server_mutex;
constexpr int DEFAULT_SERVER_PORT = 8815;

void StartDistributedServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	if (g_server_started) {
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

	try {
		g_test_server = make_uniq<DistributedFlightServer>("0.0.0.0", port);
		auto status = g_test_server->Start();
		if (!status.ok()) {
			throw Exception(ExceptionType::IO, "Failed to start server: " + status.ToString());
		}

		// Start server in background thread (detached to avoid cleanup issues).
		g_server_thread = make_uniq<std::thread>([port]() {
			// This thread owns its own server instance
			auto serve_status = g_test_server->Serve();
			if (!serve_status.ok() && g_server_started) {
				std::cerr << "Server error on port " << port << ": " << serve_status.ToString() << std::endl;
			}
		});
		
		// Detach the thread so it doesn't need cleanup
		g_server_thread->detach();
		
		// TODO(hjiang): Use readiness probe to validate server on.
		std::this_thread::sleep_for(std::chrono::milliseconds(500));

		g_server_started = true;

		auto result_data = FlatVector::GetData<string_t>(result);
		result_data[0] = StringVector::AddString(result, "Distributed server started on port " + std::to_string(port));

		// Note: Cleanup happens via detached thread or explicit stop_distributed_server()

	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start distributed server: " + string(ex.what()));
	}
}

void StopDistributedServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

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

}  // namespace

ScalarFunction GetStartDistributedServerFunction() {
	auto start_func = ScalarFunction("start_distributed_server", 
									  /*arguments*/{LogicalType::INTEGER}, 
									  /*return_type=*/LogicalType::VARCHAR,
	                                  StartDistributedServer);
	start_func.varargs = LogicalType::ANY;
	return start_func;
}

ScalarFunction GetStopDistributedServerFunction() {
	return ScalarFunction("stop_distributed_server", 
							/*arguments*/{}, 
							/*return_type=*/LogicalType::VARCHAR,
	                      StopDistributedServer);
}

} // namespace duckdb
