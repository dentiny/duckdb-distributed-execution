#include "utils/readiness_probe.hpp"

#include "distributed.pb.h"
#include <arrow/buffer.h>
#include <arrow/flight/api.h>
#include <chrono>
#include <thread>

namespace duckdb {

namespace {
// timeout_milliseconds for individual readiness checks
constexpr int CHECK_TIMEOUT_MS = 500;
// timeout_milliseconds for waiting for driver server to become ready
constexpr int DRIVER_WAIT_TIMEOUT_MS = 10000;
// Poll interval for waiting for driver server
constexpr int DRIVER_POLL_INTERVAL_MS = 100;
// timeout_milliseconds for waiting for worker node to become ready
constexpr int WORKER_WAIT_TIMEOUT_MS = 5000;
// Poll interval for waiting for worker node
constexpr int WORKER_POLL_INTERVAL_MS = 100;

bool IsDriverServerReady(const string &location) {
	try {
		// Parse the location
		arrow::flight::Location flight_location;
		auto parse_result = arrow::flight::Location::Parse(location);
		if (!parse_result.ok()) {
			return false;
		}
		flight_location = std::move(parse_result.ValueOrDie());

		// Attempt to connect
		std::unique_ptr<arrow::flight::FlightClient> client;
		auto connect_result = arrow::flight::FlightClient::Connect(flight_location);
		if (!connect_result.ok()) {
			return false;
		}
		client = std::move(connect_result.ValueOrDie());

		// Send ReadinessCheck action to verify the server is ready
		distributed::DistributedRequest req;
		req.mutable_readiness_check();

		std::string req_data = req.SerializeAsString();
		arrow::flight::Action action;
		action.type = "execute";
		action.body = arrow::Buffer::FromString(req_data);

		// Send action and get results
		auto action_result = client->DoAction(action);
		if (!action_result.ok()) {
			return false;
		}

		// Get the first result
		auto results = std::move(action_result.ValueOrDie());
		auto next_result = results->Next();
		if (!next_result.ok()) {
			return false;
		}
		auto result = std::move(next_result.ValueOrDie());
		if (result == nullptr) {
			return false;
		}

		// Parse the response to verify it's a valid ReadinessCheck response
		distributed::DistributedResponse resp;
		if (!resp.ParseFromArray(result->body->data(), result->body->size())) {
			return false;
		}

		// Check if the response indicates the server is ready
		return resp.success() && resp.has_readiness_check() && resp.readiness_check().ready();
	} catch (...) {
		return false;
	}
}

bool IsWorkerNodeReady(const string &location) {
	try {
		// Parse the location
		arrow::flight::Location flight_location;
		auto parse_result = arrow::flight::Location::Parse(location);
		if (!parse_result.ok()) {
			return false;
		}
		flight_location = std::move(parse_result.ValueOrDie());

		// Attempt to connect (timeout is handled at the OS level for connection)
		std::unique_ptr<arrow::flight::FlightClient> client;
		auto connect_result = arrow::flight::FlightClient::Connect(flight_location);
		if (!connect_result.ok()) {
			return false;
		}
		client = std::move(connect_result.ValueOrDie());

		distributed::DistributedRequest req;
		req.mutable_worker_heartbeat();

		std::string req_data = req.SerializeAsString();
		arrow::flight::Action action;
		action.type = "execute";
		action.body = arrow::Buffer::FromString(req_data);

		auto action_result = client->DoAction(action);
		if (!action_result.ok()) {
			return false;
		}

		// Get the first result
		auto results = std::move(action_result.ValueOrDie());
		auto next_result = results->Next();
		if (!next_result.ok()) {
			return false;
		}
		auto result = std::move(next_result.ValueOrDie());
		if (result == nullptr) {
			return false;
		}

		distributed::DistributedResponse resp;
		if (!resp.ParseFromArray(result->body->data(), result->body->size())) {
			return false;
		}

		// Check if the response indicates the worker is healthy.
		return resp.success() && resp.has_worker_heartbeat() && resp.worker_heartbeat().healthy();
	} catch (...) {
		return false;
	}
}

} // namespace

bool WaitForDriverServerReady(const string &location) {
	auto start_time = std::chrono::steady_clock::now();
	auto timeout_milliseconds = std::chrono::milliseconds(DRIVER_WAIT_TIMEOUT_MS);
	auto poll_interval = std::chrono::milliseconds(DRIVER_POLL_INTERVAL_MS);

	while (true) {
		if (IsDriverServerReady(location)) {
			return true;
		}

		auto elapsed = std::chrono::steady_clock::now() - start_time;
		if (elapsed >= timeout_milliseconds) {
			return false;
		}

		std::this_thread::sleep_for(poll_interval);
	}
}

bool WaitForWorkerNodeReady(const string &location) {
	auto start_time = std::chrono::steady_clock::now();
	auto timeout_milliseconds = std::chrono::milliseconds(WORKER_WAIT_TIMEOUT_MS);
	auto poll_interval = std::chrono::milliseconds(WORKER_POLL_INTERVAL_MS);

	while (true) {
		if (IsWorkerNodeReady(location)) {
			return true;
		}

		auto elapsed = std::chrono::steady_clock::now() - start_time;
		if (elapsed >= timeout_milliseconds) {
			return false;
		}

		std::this_thread::sleep_for(poll_interval);
	}
}

} // namespace duckdb
