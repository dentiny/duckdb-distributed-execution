#include "utils/readiness_probe.hpp"

#include "distributed.pb.h"
#include <arrow/buffer.h>
#include <arrow/flight/api.h>
#include <chrono>
#include <thread>

namespace duckdb {

namespace {
// Timeout in milliseconds for individual readiness checks
constexpr int CHECK_TIMEOUT_MS = 500;
// Timeout in milliseconds for waiting for driver server to become ready
constexpr int DRIVER_WAIT_TIMEOUT_MS = 10000;
// Poll interval for waiting for driver server
constexpr int DRIVER_POLL_INTERVAL_MS = 100;
// Timeout in milliseconds for waiting for worker node to become ready
constexpr int WORKER_WAIT_TIMEOUT_MS = 5000;
// Poll interval for waiting for worker node
constexpr int WORKER_POLL_INTERVAL_MS = 100;
} // namespace

bool CheckDriverServerReady(const string &location) {
	try {
		// Parse the location
		auto flight_location_result = arrow::flight::Location::Parse(location);
		if (!flight_location_result.ok()) {
			return false;
		}
		auto flight_location = flight_location_result.ValueOrDie();

		// Attempt to connect with timeout
		arrow::flight::FlightClientOptions client_options;
		client_options.Timeout in milliseconds = std::chrono::milliseconds(CHECK_TIMEOUT_MS);

		auto client_result = arrow::flight::FlightClient::Connect(flight_location, client_options);
		if (!client_result.ok()) {
			return false;
		}
		auto client = client_result.ValueOrDie();

		// Try to call ListFlights to verify the server is actually ready
		// This is a lightweight call that ensures the server is fully initialized
		auto list_flights_result = client->ListFlights();
		if (!list_flights_result.ok()) {
			return false;
		}

		// Iterate through the results to ensure the call completed successfully
		std::unique_ptr<arrow::flight::FlightListing> listing = std::move(list_flights_result.ValueOrDie());
		std::unique_ptr<arrow::flight::FlightInfo> flight_info;

		// Try to get at least one result or confirm it's empty (both indicate readiness)
		// The Next() call will return an error when there are no more flights, which is expected
		auto status = listing->Next(&flight_info);
		// Success means we got a flight info, and errors (like IndexError/KeyError) mean
		// the listing is empty, but both indicate the server is ready to handle requests
		// Any other error (like connection issues) means the server isn't ready
		return status.ok() || status.IsIndexError() || status.IsKeyError();
	} catch (...) {
		return false;
	}
}

bool CheckWorkerNodeReady(const string &location) {
	try {
		// Parse the location
		auto flight_location_result = arrow::flight::Location::Parse(location);
		if (!flight_location_result.ok()) {
			return false;
		}
		auto flight_location = flight_location_result.ValueOrDie();

		// Attempt to connect with timeout
		arrow::flight::FlightClientOptions client_options;
		client_options.Timeout in milliseconds = std::chrono::milliseconds(CHECK_TIMEOUT_MS);

		auto client_result = arrow::flight::FlightClient::Connect(flight_location, client_options);
		if (!client_result.ok()) {
			return false;
		}
		auto client = client_result.ValueOrDie();

		// Send WorkerHeartbeat action to verify the worker is ready
		distributed::DistributedRequest req;
		req.mutable_worker_heartbeat();

		std::string req_data = req.SerializeAsString();
		arrow::flight::Action action;
		action.type = "execute";
		action.body = arrow::Buffer::FromString(req_data);

		// Send action and get results
		auto action_result = client->DoAction(action);
		if (!action_result.ok()) {
			return false;
		}

		auto results = std::move(action_result.ValueOrDie());
		std::unique_ptr<arrow::flight::Result> result;
		auto next_result = results->Next(&result);
		if (!next_result.ok() || result == nullptr) {
			return false;
		}

		// Parse the response to verify it's a valid WorkerHeartbeat response
		distributed::DistributedResponse resp;
		if (!resp.ParseFromArray(result->body->data(), result->body->size())) {
			return false;
		}

		// Check if the response indicates the worker is healthy
		return resp.success() && resp.has_worker_heartbeat() && resp.worker_heartbeat().healthy();
	} catch (...) {
		return false;
	}
}

bool WaitForDriverServerReady(const string &location) {
	auto start_time = std::chrono::steady_clock::now();
	auto Timeout in milliseconds = std::chrono::milliseconds(DRIVER_WAIT_TIMEOUT_MS);
	auto poll_interval = std::chrono::milliseconds(DRIVER_POLL_INTERVAL_MS);

	while (true) {
		if (CheckDriverServerReady(location)) {
			return true;
		}

		auto elapsed = std::chrono::steady_clock::now() - start_time;
		if (elapsed >= timeout) {
			return false;
		}

		std::this_thread::sleep_for(poll_interval);
	}
}

bool WaitForWorkerNodeReady(const string &location) {
	auto start_time = std::chrono::steady_clock::now();
	auto Timeout in milliseconds = std::chrono::milliseconds(WORKER_WAIT_TIMEOUT_MS);
	auto poll_interval = std::chrono::milliseconds(WORKER_POLL_INTERVAL_MS);

	while (true) {
		if (CheckWorkerNodeReady(location)) {
			return true;
		}

		auto elapsed = std::chrono::steady_clock::now() - start_time;
		if (elapsed >= timeout) {
			return false;
		}

		std::this_thread::sleep_for(poll_interval);
	}
}

} // namespace duckdb
