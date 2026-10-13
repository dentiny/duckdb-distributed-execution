#define DUCKDB_EXTENSION_MAIN

#include "client/execution/distributed_aggregate_pushdown.hpp"
#include "client/execution/distributed_query_pushdown.hpp"
#include "client/execution/logical_remote_alter_table.hpp"
#include "client/execution/logical_remote_create_index.hpp"
#include "duckdb.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckherder_extension.hpp"
#include "duckherder_functions.hpp"
#include "duckherder_extension_instance_state.hpp"
#include "duckherder_storage.hpp"
#include "utils/retry_utils.hpp"

namespace duckdb {

namespace {

void SetAsyncThreads(ClientContext &, SetScope scope, Value &parameter) {
	if (scope != SetScope::GLOBAL) {
		throw InvalidInputException("duckherder_async_threads must be set globally");
	}
	const auto threads = parameter.IsNull() ? -1 : parameter.GetValue<int64_t>();
	if (threads < 0 || threads > NumericLimits<int32_t>::Maximum()) {
		throw InvalidInputException("duckherder_async_threads must be between 0 and INT_MAX");
	}
}

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "duckherder", make_shared_ptr<DuckherderStorageExtension>());
	OperatorExtension::Register(config, GetRemoteAlterTableOperatorExtension());
	OperatorExtension::Register(config, GetRemoteCreateIndexOperatorExtension());
	// Whole queries are pushed down first; aggregates are pushed down only from queries that stay on the client.
	OptimizerExtension::Register(config, GetDistributedQueryPushdownExtension());
	OptimizerExtension::Register(config, GetDistributedAggregatePushdownExtension());
	config.AddExtensionOption(QUERY_PUSHDOWN_SETTING, "Push whole queries reading only remote tables to the server",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption(RETRY_MAX_ATTEMPTS_SETTING, "Maximum number of attempts for retryable Duckherder RPCs",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_RETRY_MAX_ATTEMPTS), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption(RETRY_INITIAL_BACKOFF_MS_SETTING, "Initial Duckherder RPC retry backoff in milliseconds",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_RETRY_INITIAL_BACKOFF_MS), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption(RETRY_MAX_BACKOFF_MS_SETTING, "Maximum Duckherder RPC retry backoff in milliseconds",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_RETRY_MAX_BACKOFF_MS), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption(RETRY_JITTER_RATIO_SETTING, "Jitter ratio applied to Duckherder RPC retry backoff",
	                          LogicalType::DOUBLE, Value::DOUBLE(DEFAULT_RETRY_JITTER_RATIO), nullptr,
	                          SetScope::GLOBAL);

	config.AddExtensionOption("duckherder_async_threads",
	                          "Worker RPC thread limit (0: min(4 * DuckDB threads, 256)); applied on next dispatch",
	                          LogicalType::BIGINT, Value::BIGINT(0), SetAsyncThreads, SetScope::GLOBAL);

	// Set extension state.
	SetInstanceState(db, make_shared_ptr<DuckherderInstanceState>());

	RegisterDuckherderFunctions(loader);
}

} // namespace

void DuckherderExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DuckherderExtension::Name() {
	return "duckherder";
}

std::string DuckherderExtension::Version() const {
#ifdef EXT_VERSION_DUCKHERDER
	return EXT_VERSION_DUCKHERDER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckherder, loader) {
	duckdb::LoadInternal(loader);
}
}
