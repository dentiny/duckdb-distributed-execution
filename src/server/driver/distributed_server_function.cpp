#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "server/driver/distributed_flight_server.hpp"
#include "utils/network_utils.hpp"
#include "utils/no_destructor.hpp"
#include "utils/thread_utils.hpp"

#include <thread>

namespace duckdb {

namespace {

// Success function return value.
constexpr bool SUCCESS = true;
// Default server port to start use from.
constexpr int DEFAULT_SERVER_PORT = 8815;

// Global state for test servers and workers; wrap them in NoDestructor to avoid destruct
struct TestServerState {
	unique_ptr<DistributedFlightServer> test_server;
	unique_ptr<std::thread> server_thread;
	unordered_map<int, unique_ptr<WorkerNode>> standalone_workers;
	unordered_map<int, unique_ptr<std::thread>> worker_threads;
	int next_standalone_worker_port = 9000;
};

static TestServerState &GetTestServerState() {
	static NoDestructor<TestServerState> state {};
	return *state;
}

void StartLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &server_state = GetTestServerState();

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

	// If server already exists, properly shut it down first.
	if (server_state.test_server != nullptr) {
		// Shutdown the server and wait for the thread to finish.
		server_state.test_server->Shutdown();
		if (server_state.server_thread != nullptr && server_state.server_thread->joinable()) {
			server_state.server_thread->join();
		}
		server_state.server_thread.reset();

		// Shutdown standalone workers and wait for their threads.
		for (auto &[worker_port, worker] : server_state.standalone_workers) {
			worker->Shutdown();
		}
		for (auto &[worker_port, thd] : server_state.worker_threads) {
			if (thd != nullptr && thd->joinable()) {
				thd->join();
			}
		}
		server_state.standalone_workers.clear();
		server_state.worker_threads.clear();
		server_state.test_server.reset();
	}

	// Create new server instance
	if (server_state.test_server == nullptr) {
		server_state.test_server = make_uniq<DistributedFlightServer>("0.0.0.0", port);
	}

	arrow::Status status;
	if (worker_count > 0) {
		status = server_state.test_server->StartWithWorkers(worker_count);
	} else {
		status = server_state.test_server->Start();
	}
	if (!status.ok()) {
		throw Exception(ExceptionType::IO, "Failed to start local server: " + status.ToString());
	}

	// Start server in background thread.
	server_state.server_thread = make_uniq<std::thread>([server_ptr = server_state.test_server.get(), port]() {
		SetThreadName("LocalDuckSrv");

		auto serve_status = server_ptr->Serve();
		if (!serve_status.ok()) {
			throw IOException(StringUtil::Format("Failed to start driver node: %s", serve_status.ToString()));
		}
	});

	// TODO(hjiang): Should use readiness probe to wait until ready.
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));

	result.Reference(Value(SUCCESS));
}

void StopLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &server_state = GetTestServerState();

	if (server_state.test_server != nullptr) {
		// Shutdown the server
		server_state.test_server->Shutdown();

		// Wait for the server thread to finish
		if (server_state.server_thread != nullptr && server_state.server_thread->joinable()) {
			server_state.server_thread->join();
		}
		server_state.server_thread.reset();

		// Shutdown standalone workers and wait for their threads
		for (auto &[worker_port, worker] : server_state.standalone_workers) {
			worker->Shutdown();
		}
		for (auto &[worker_port, thread] : server_state.worker_threads) {
			if (thread != nullptr && thread->joinable()) {
				thread->join();
			}
		}
		server_state.standalone_workers.clear();
		server_state.worker_threads.clear();

		server_state.test_server.reset();
	}
	result.Reference(Value(SUCCESS));
}

void GetWorkerCount(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &server_state = GetTestServerState();

	if (server_state.test_server == nullptr) {
		result.SetValue(0, Value::BIGINT(0));
		return;
	}

	const idx_t count = server_state.test_server->GetWorkerCount();
	result.SetValue(0, Value::BIGINT(count));
}

void RegisterWorker(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &server_state = GetTestServerState();

	if (server_state.test_server == nullptr) {
		throw Exception(ExceptionType::INVALID_INPUT,
		                "Server not started. Call duckherder_start_local_server() first.");
	}

	auto &worker_id_vector = args.data[0];
	auto &location_vector = args.data[1];

	auto worker_id_data = FlatVector::GetData<string_t>(worker_id_vector);
	auto location_data = FlatVector::GetData<string_t>(location_vector);

	string worker_id = worker_id_data[0].GetString();
	string location = location_data[0].GetString();

	server_state.test_server->RegisterWorker(worker_id, location);
	result.Reference(Value(SUCCESS));
}

void RegisterOrReplaceDriver(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &server_state = GetTestServerState();

	if (server_state.test_server == nullptr) {
		throw Exception(ExceptionType::INVALID_INPUT,
		                "Server not started. Call duckherder_start_local_server() first.");
	}

	auto &driver_id_vector = args.data[0];
	auto &location_vector = args.data[1];
	auto driver_id_data = FlatVector::GetData<string_t>(driver_id_vector);
	auto location_data = FlatVector::GetData<string_t>(location_vector);
	string driver_id = driver_id_data[0].GetString();
	string location = location_data[0].GetString();

	server_state.test_server->RegisterOrReplaceDriver(driver_id, location);
	result.Reference(Value(SUCCESS));
}

void StartStandaloneWorker(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &server_state = GetTestServerState();

	int port = 0;

	// If port is provided, use it; otherwise, find an available port.
	if (args.ColumnCount() > 0 && args.size() > 0) {
		D_ASSERT(args.ColumnCount() == 1);
		D_ASSERT(args.size() == 1);
		auto &port_vector = args.data[0];
		auto port_data = FlatVector::GetData<int32_t>(port_vector);
		port = port_data[0];
	} else {
		// Find an available port starting from the next tracked port.
		port = GetAvailablePort(server_state.next_standalone_worker_port);
		if (port < 0) {
			throw IOException("Failed to find available port for standalone worker");
		}
	}

	auto existing = server_state.standalone_workers.find(port);
	if (existing != server_state.standalone_workers.end()) {
		throw InvalidInputException(StringUtil::Format("Worker node with port %d has already been registered.", port));
	}

	string worker_id = StringUtil::Format("standalone_worker_%d", port);
	auto worker = make_uniq<WorkerNode>(worker_id, "localhost", port, nullptr);

	auto status = worker->Start();
	if (!status.ok()) {
		throw IOException(StringUtil::Format("Failed to start standalone worker: %s", status.ToString()));
	}

	auto *worker_ptr = worker.get();
	server_state.standalone_workers[port] = std::move(worker);

	// Update next port for future auto-assignment.
	server_state.next_standalone_worker_port = port + 1;

	// Start worker in background thread.
	server_state.worker_threads[port] = make_uniq<std::thread>([worker_ptr, port]() {
		SetThreadName("StandaloneWkr");
		auto serve_status = worker_ptr->Serve();
		if (!serve_status.ok()) {
			throw IOException(StringUtil::Format("Failed to start worker node: %s", serve_status.ToString()));
		}
	});

	// TODO(hjiang): Use readiness probe to validate worker node up.
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	result.Reference(Value(SUCCESS));
}

} // namespace

ScalarFunction GetStartLocalServerFunction() {
	auto start_func = ScalarFunction("duckherder_start_local_server",
	                                 /*arguments=*/ {LogicalType {LogicalTypeId::INTEGER}},
	                                 /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StartLocalServer);
	start_func.varargs = LogicalType {LogicalTypeId::INTEGER};
	return start_func;
}

ScalarFunction GetStopLocalServerFunction() {
	return ScalarFunction("duckherder_stop_local_server",
	                      /*arguments=*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StopLocalServer);
}

ScalarFunction GetWorkerCountFunction() {
	return ScalarFunction("duckherder_get_worker_count",
	                      /*arguments=*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, GetWorkerCount);
}

ScalarFunction GetRegisterWorkerFunction() {
	return ScalarFunction("duckherder_register_worker",
	                      /*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, RegisterWorker);
}

ScalarFunction GetStartStandaloneWorkerFunction() {
	auto start_func = ScalarFunction("duckherder_start_standalone_worker",
	                                 /*arguments=*/ {LogicalType {LogicalTypeId::INTEGER}},
	                                 /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StartStandaloneWorker);
	start_func.varargs = LogicalType {LogicalTypeId::INTEGER};
	return start_func;
}

ScalarFunction GetRegisterOrReplaceDriverFunction() {
	return ScalarFunction("duckherder_register_or_replace_driver",
	                      /*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, RegisterOrReplaceDriver);
}

} // namespace duckdb
