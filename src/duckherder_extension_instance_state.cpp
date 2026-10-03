#include "duckherder_extension_instance_state.hpp"

#include "query_recorder.hpp"

namespace duckdb {

DuckherderInstanceState::DuckherderInstanceState() : query_recorder(make_shared_ptr<QueryRecorder>()) {
}

shared_ptr<BaseQueryRecorder> DuckherderInstanceState::GetQueryRecorder() const {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	D_ASSERT(query_recorder != nullptr);
	return query_recorder;
}

void SetInstanceState(DatabaseInstance &instance, shared_ptr<DuckherderInstanceState> state) {
	instance.GetObjectCache().Put(DuckherderInstanceState::CACHE_KEY, std::move(state));
}

DuckherderInstanceState &GetDuckherderInstanceStateOrThrow(ClientContext &client_context) {
	auto &duckdb_instance = client_context.db;
	return GetDuckherderInstanceStateOrThrow(*duckdb_instance);
}

DuckherderInstanceState &GetDuckherderInstanceStateOrThrow(DatabaseInstance &instance) {
	auto state = instance.GetObjectCache().Get<DuckherderInstanceState>(DuckherderInstanceState::CACHE_KEY);
	if (state == nullptr) {
		throw InternalException("duckherder instance state not found - extension not properly loaded");
	}
	return *state;
}

} // namespace duckdb
