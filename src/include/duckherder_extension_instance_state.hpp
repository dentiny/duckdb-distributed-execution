// Per-instance state for duckherder extension.
// State is stored in DuckDB's ObjectCache for automatic cleanup when DatabaseInstance is destroyed.

#pragma once

#include <mutex>

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "base_query_recorder.hpp"

namespace duckdb {

// Forward declaration.
class ClientContext;
class DatabaseInstance;

//===--------------------------------------------------------------------===//
// Main per-instance state container
// Inherits from ObjectCacheEntry for automatic cleanup when DatabaseInstance is destroyed
//===--------------------------------------------------------------------===//
struct DuckherderInstanceState : public ObjectCacheEntry {
	static constexpr const char *OBJECT_TYPE = "DuckherderInstanceState";
	static constexpr const char *CACHE_KEY = "duckherder_instance_state";

	DuckherderInstanceState();

	// Get the current query recorder.
	shared_ptr<BaseQueryRecorder> GetQueryRecorder() const;

	// ObjectCacheEntry interface
	string GetObjectType() override {
		return OBJECT_TYPE;
	}

	static string ObjectType() {
		return OBJECT_TYPE;
	}

private:
	mutable std::mutex mu;
	shared_ptr<BaseQueryRecorder> query_recorder;
};

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

// Store instance state in DatabaseInstance
void SetInstanceState(DatabaseInstance &instance, shared_ptr<DuckherderInstanceState> state);

// Get instance state, throwing if not found
DuckherderInstanceState &GetInstanceStateOrThrow(ClientContext &client_context);
DuckherderInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance);

} // namespace duckdb
