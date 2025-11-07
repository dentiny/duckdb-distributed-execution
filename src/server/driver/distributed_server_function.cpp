#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "server/driver/distributed_flight_server.hpp"
#include "utils/thread_utils.hpp"

#include <thread>

namespace duckdb {

namespace {

// Success function return value.
constexpr bool SUCCESS = true;

// Global server instance and thread.
unique_ptr<DistributedFlightServer> g_test_server;
bool g_server_started = false;
std::mutex g_server_mutex;
constexpr int DEFAULT_SERVER_PORT = 8815;

void StartLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	if (g_server_started) {
		result.Reference(Value(SUCCESS));
		return;
	}

	int port = DEFAULT_SERVER_PORT;
	int worker_count = 0;
	if (args.ColumnCount() > 0 && args.size() > 0) {
		auto &port_vector = args.data[0];
		auto port_data = FlatVector::GetData<int32_t>(port_vector);
		port = port_data[0];
	}
	if (args.ColumnCount() > 1 && args.size() > 0) {
		auto &worker_vector = args.data[1];
		auto worker_data = FlatVector::GetData<int32_t>(worker_vector);
		worker_count = worker_data[0];
	}

	try {
		g_test_server = make_uniq<DistributedFlightServer>("0.0.0.0", port);
		arrow::Status status;
		if (worker_count > 0) {
			status = g_test_server->StartWithWorkers(worker_count);
		} else {
			status = g_test_server->Start();
		}
		if (!status.ok()) {
			throw Exception(ExceptionType::IO, "Failed to start local server: " + status.ToString());
		}

		// Start server in background thread and detach.
		std::thread([port]() {
			SetThreadName("LocalDuckSrv");

			// This thread owns its own server instance
			auto serve_status = g_test_server->Serve();
			if (!serve_status.ok() && g_server_started) {
				std::cerr << "Server error on port " << port << ": " << serve_status.ToString() << std::endl;
			}
		}).detach();

		// TODO(hjiang): Use readiness probe to validate server on.
		std::this_thread::sleep_for(std::chrono::milliseconds(500));

		g_server_started = true;
		result.Reference(Value(SUCCESS));
	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start local server: " + string(ex.what()));
	}
}

void StopLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	if (!g_server_started) {
		result.Reference(Value(SUCCESS));
		return;
	}

	if (g_test_server != nullptr) {
		g_test_server->Shutdown();
	}

	g_test_server.reset();
	g_server_started = false;

	result.Reference(Value(SUCCESS));
}

} // namespace

ScalarFunction GetStartLocalServerFunction() {
	auto start_func = ScalarFunction("duckherder_start_local_server",
	                                 /*arguments*/ {LogicalType {LogicalTypeId::INTEGER}},
	                                 /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StartLocalServer);
	start_func.varargs = LogicalType::ANY;
	return start_func;
}

ScalarFunction GetStopLocalServerFunction() {
	return ScalarFunction("duckherder_stop_local_server",
	                      /*arguments*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StopLocalServer);
}

} // namespace duckdb
