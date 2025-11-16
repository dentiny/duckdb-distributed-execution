#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "server/driver/distributed_flight_server.hpp"
#include "utils/thread_utils.hpp"

#include <thread>
#include <unordered_map>

namespace duckdb {

namespace {

// Success function return value.
constexpr bool SUCCESS = true;

// Map of port -> server instance for test isolation
std::unordered_map<int, unique_ptr<DistributedFlightServer>> g_test_servers;
std::mutex g_server_mutex;
constexpr int DEFAULT_SERVER_PORT = 8815;

// Map of port -> standalone worker instance for testing registration
std::unordered_map<int, unique_ptr<WorkerNode>> g_standalone_workers;
std::mutex g_worker_mutex;

void StartLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

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

	// Check if server already exists on this port
	// If it does, shut it down and create a new one for test isolation
	auto existing = g_test_servers.find(port);
	if (existing != g_test_servers.end()) {
		if (existing->second) {
			existing->second->Shutdown();
		}
		g_test_servers.erase(existing);
		// Small delay to ensure port is released
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	try {
		auto server = make_uniq<DistributedFlightServer>("0.0.0.0", port);
		arrow::Status status;
		if (worker_count > 0) {
			status = server->StartWithWorkers(worker_count);
		} else {
			status = server->Start();
		}
		if (!status.ok()) {
			throw Exception(ExceptionType::IO, "Failed to start local server: " + status.ToString());
		}

		// Store the server in the map
		auto* server_ptr = server.get();
		g_test_servers[port] = std::move(server);

		// Start server in background thread and detach.
		std::thread([server_ptr, port]() {
			SetThreadName("LocalDuckSrv");

			auto serve_status = server_ptr->Serve();
			if (!serve_status.ok()) {
				// Server stopped, remove from map
				std::lock_guard<std::mutex> lock(g_server_mutex);
				g_test_servers.erase(port);
			}
		}).detach();

		// TODO(hjiang): Use readiness probe to validate server on.
		std::this_thread::sleep_for(std::chrono::milliseconds(500));

		result.Reference(Value(SUCCESS));
	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start local server: " + string(ex.what()));
	}
}

void StopLocalServer(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	// Stop all servers
	for (auto &entry : g_test_servers) {
		if (entry.second) {
			entry.second->Shutdown();
		}
	}
	g_test_servers.clear();

	result.Reference(Value(SUCCESS));
}

void GetWorkerCount(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	// Use the default port server
	auto it = g_test_servers.find(DEFAULT_SERVER_PORT);
	if (it == g_test_servers.end() || !it->second) {
		result.SetValue(0, Value::BIGINT(0));
		return;
	}

	idx_t count = it->second->GetWorkerCount();
	result.SetValue(0, Value::BIGINT(count));
}

void StartLocalWorkers(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	auto it = g_test_servers.find(DEFAULT_SERVER_PORT);
	if (it == g_test_servers.end() || !it->second) {
		throw Exception(ExceptionType::INVALID_INPUT,
		                "Server not started. Call duckherder_start_local_server() first.");
	}

	auto &num_workers_vector = args.data[0];
	auto num_workers_data = FlatVector::GetData<int32_t>(num_workers_vector);
	int num_workers = num_workers_data[0];

	if (num_workers <= 0) {
		throw Exception(ExceptionType::INVALID_INPUT, "Number of workers must be positive");
	}

	try {
		// Add workers to the existing server
		it->second->StartLocalWorkers(num_workers);
		result.Reference(Value(SUCCESS));
	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start local workers: " + string(ex.what()));
	}
}

void RegisterWorker(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	auto it = g_test_servers.find(DEFAULT_SERVER_PORT);
	if (it == g_test_servers.end() || !it->second) {
		throw Exception(ExceptionType::INVALID_INPUT,
		                "Server not started. Call duckherder_start_local_server() first.");
	}

	auto &worker_id_vector = args.data[0];
	auto &location_vector = args.data[1];

	auto worker_id_data = FlatVector::GetData<string_t>(worker_id_vector);
	auto location_data = FlatVector::GetData<string_t>(location_vector);

	string worker_id = worker_id_data[0].GetString();
	string location = location_data[0].GetString();

	try {
		it->second->RegisterWorker(worker_id, location);
		result.Reference(Value(SUCCESS));
	} catch (const Exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to register worker: " + string(ex.what()));
	}
}

void RegisterWorkers(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_server_mutex);

	auto it = g_test_servers.find(DEFAULT_SERVER_PORT);
	if (it == g_test_servers.end() || !it->second) {
		throw Exception(ExceptionType::INVALID_INPUT,
		                "Server not started. Call duckherder_start_local_server() first.");
	}

	auto &worker_ids_vector = args.data[0];
	auto &locations_vector = args.data[1];

	// Get list entries
	auto &worker_ids_entry = ListVector::GetEntry(worker_ids_vector);
	auto &locations_entry = ListVector::GetEntry(locations_vector);

	auto worker_ids_data = FlatVector::GetData<string_t>(worker_ids_entry);
	auto locations_data = FlatVector::GetData<string_t>(locations_entry);

	auto worker_ids_size = ListVector::GetListSize(worker_ids_vector);
	auto locations_size = ListVector::GetListSize(locations_vector);

	if (worker_ids_size != locations_size) {
		throw Exception(ExceptionType::INVALID_INPUT,
		                StringUtil::Format("Worker IDs and locations arrays must have the same length (got %llu and %llu)",
		                                   worker_ids_size, locations_size));
	}

	// Register all workers, continue on failures
	idx_t successful_count = 0;
	for (idx_t i = 0; i < worker_ids_size; i++) {
		string worker_id = worker_ids_data[i].GetString();
		string location = locations_data[i].GetString();
		
		try {
			it->second->RegisterWorker(worker_id, location);
			successful_count++;
		} catch (const Exception &ex) {
			// Log failure but continue with other workers
			// Individual failures don't stop the batch
		}
	}

	result.SetValue(0, Value::BIGINT(successful_count));
}

void StartStandaloneWorker(DataChunk &args, ExpressionState &state, Vector &result) {
	const std::lock_guard<std::mutex> lock(g_worker_mutex);

	// Get port for the standalone worker
	auto &port_vector = args.data[0];
	auto port_data = FlatVector::GetData<int32_t>(port_vector);
	int port = port_data[0];

	// Check if a worker already exists on this port
	auto existing = g_standalone_workers.find(port);
	if (existing != g_standalone_workers.end()) {
		if (existing->second) {
			existing->second->Shutdown();
		}
		g_standalone_workers.erase(existing);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	try {
		string worker_id = StringUtil::Format("standalone_worker_%d", port);
		auto worker = make_uniq<WorkerNode>(worker_id, "localhost", port, nullptr);
		
		auto status = worker->Start();
		if (!status.ok()) {
			throw Exception(ExceptionType::IO, "Failed to start standalone worker: " + status.ToString());
		}

		// Store the worker
		auto* worker_ptr = worker.get();
		g_standalone_workers[port] = std::move(worker);

		// Start worker in background thread
		std::thread([worker_ptr, port]() {
			SetThreadName("StandaloneWkr");
			auto serve_status = worker_ptr->Serve();
			if (!serve_status.ok()) {
				// Worker stopped, remove from map
				std::lock_guard<std::mutex> lock(g_worker_mutex);
				g_standalone_workers.erase(port);
			}
		}).detach();

		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		result.Reference(Value(SUCCESS));
	} catch (const std::exception &ex) {
		throw Exception(ExceptionType::IO, "Failed to start standalone worker: " + string(ex.what()));
	}
}

} // namespace

ScalarFunction GetStartLocalServerFunction() {
	auto start_func = ScalarFunction("duckherder_start_local_server",
	                                 /*arguments*/ {LogicalType {LogicalTypeId::INTEGER}},
	                                 /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StartLocalServer);
	start_func.varargs = LogicalType {LogicalTypeId::INTEGER};
	return start_func;
}

ScalarFunction GetStopLocalServerFunction() {
	return ScalarFunction("duckherder_stop_local_server",
	                      /*arguments*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StopLocalServer);
}

ScalarFunction GetWorkerCountFunction() {
	return ScalarFunction("duckherder_get_worker_count",
	                      /*arguments*/ {},
	                      /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, GetWorkerCount);
}

ScalarFunction GetStartLocalWorkersFunction() {
	return ScalarFunction("duckherder_start_local_workers",
	                      /*arguments*/ {LogicalType {LogicalTypeId::INTEGER}},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StartLocalWorkers);
}

ScalarFunction GetRegisterWorkerFunction() {
	return ScalarFunction("duckherder_register_worker",
	                      /*arguments*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, RegisterWorker);
}

ScalarFunction GetStartStandaloneWorkerFunction() {
	return ScalarFunction("duckherder_start_standalone_worker",
	                      /*arguments*/ {LogicalType {LogicalTypeId::INTEGER}},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, StartStandaloneWorker);
}

} // namespace duckdb
