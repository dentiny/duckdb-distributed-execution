#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckherder_functions.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_fragment_pushdown.hpp"
#include "server/driver/worker_manager.hpp"

namespace duckdb {

namespace {

// SQL test wiring: ordinary client SQL cannot create attachments in a remote driver's context.
struct WorkerFragmentTestState : public ClientContextState {
	static constexpr const char *NAME = "worker_fragment_test";

	WorkerFragmentTestState(ClientContext &context, distributed::StorageConfig config, idx_t workers)
	    : db(*context.db), manager(db), connection(db) {
		auto result = connection.Query(
		    "USE " + KeywordHelper::WriteOptionallyQuoted(DatabaseManager::GetDefaultDatabase(context)));
		if (result->HasError()) {
			result->ThrowError();
		}
		auto status = manager.StartLocalWorkers(workers);
		if (!status.ok()) {
			throw IOException(status.ToString());
		}
		executor = make_uniq<DistributedExecutor>(manager, connection, std::move(config));
		fragments = make_shared_ptr<WorkerFragmentState>(*executor, connection);
	}

	DuckDB db;
	WorkerManager manager;
	Connection connection;
	unique_ptr<DistributedExecutor> executor;
	shared_ptr<WorkerFragmentState> fragments;
};

void EnableWorkerFragments(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	if (context.registered_state->Get<WorkerFragmentTestState>(WorkerFragmentTestState::NAME)) {
		throw InvalidInputException("Worker fragment test state is already enabled");
	}
	Value backend, root;
	if (!context.TryGetCurrentSetting("duckdb_objfs_backend", backend) || backend.ToString() != "local" ||
	    !context.TryGetCurrentSetting("duckdb_objfs_root", root) || root.IsNull()) {
		throw InvalidInputException("Worker fragment SQL tests require local object storage with a root");
	}
	distributed::StorageConfig config;
	config.set_database_uri(args.GetValue(0, 0).ToString());
	config.mutable_local()->set_root(root.ToString());
	auto fixture =
	    make_shared_ptr<WorkerFragmentTestState>(context, std::move(config), args.GetValue(1, 0).GetValue<uint64_t>());
	context.registered_state->Insert(WorkerFragmentTestState::NAME, fixture);
	context.registered_state->Insert(WorkerFragmentState::NAME, fixture->fragments);
	auto &db_config = DBConfig::GetConfig(*context.db);
	db_config.options.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
	OptimizerExtension::Register(db_config, GetWorkerFragmentExtension());
	result.Reference(Value::BOOLEAN(true));
}

void DisableWorkerFragments(DataChunk &, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	context.registered_state->Remove(WorkerFragmentState::NAME);
	context.registered_state->Remove(WorkerFragmentTestState::NAME);
	result.Reference(Value::BOOLEAN(true));
}

struct FragmentExecutions : public GlobalTableFunctionState {
	vector<QueryExecutionInfo> executions;
	idx_t offset = 0;
};

unique_ptr<FunctionData> BindExecutions(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                        vector<string> &names) {
	types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BIGINT};
	names = {"sql", "partitioned", "num_tasks_generated"};
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> InitExecutions(ClientContext &context, TableFunctionInitInput &) {
	auto state = context.registered_state->Get<WorkerFragmentState>(WorkerFragmentState::NAME);
	if (!state) {
		throw InvalidInputException("Worker fragment SQL test state is not enabled");
	}
	auto result = make_uniq<FragmentExecutions>();
	result->executions = state->TakeExecutions();
	return std::move(result);
}

void ScanExecutions(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<FragmentExecutions>();
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.executions.size() - state.offset);
	for (idx_t row = 0; row < count; ++row) {
		auto &execution = state.executions[state.offset++];
		output.SetValue(0, row, Value(execution.sql));
		output.SetValue(1, row, Value::BOOLEAN(execution.execution_mode == QueryExecutionMode::ROW_GROUP_PARTITION));
		output.SetValue(2, row, Value::BIGINT(execution.num_tasks_generated));
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterWorkerFragmentTestFunctions(ExtensionLoader &loader) {
	ScalarFunction enable("duckherder_test_enable_worker_fragments", {LogicalType::VARCHAR, LogicalType::UBIGINT},
	                      LogicalType::BOOLEAN, EnableWorkerFragments);
	enable.SetVolatile();
	loader.RegisterFunction(enable);
	ScalarFunction disable("duckherder_test_disable_worker_fragments", {}, LogicalType::BOOLEAN,
	                       DisableWorkerFragments);
	disable.SetVolatile();
	loader.RegisterFunction(disable);
	loader.RegisterFunction(
	    TableFunction("duckherder_test_worker_fragments", {}, ScanExecutions, BindExecutions, InitExecutions));
}

} // namespace duckdb
