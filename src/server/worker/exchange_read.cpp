#include "server/worker/exchange_read.hpp"

#include "arrow_record_batch_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "server/driver/worker_node_client.hpp"
#include "utils/mutex.hpp"

#include <arrow/flight/client.h>

namespace duckdb {

namespace {

struct ExchangeReadBindData : public TableFunctionData {
	// Index of the task's exchange source to read.
	idx_t exchange = 0;
	idx_t bucket = 0;
	vector<LogicalType> types;
	idx_t estimated_cardinality = 0;
};

// Each thread reads the bucket from one producer at a time.
struct ExchangeReadGlobalState : public GlobalTableFunctionState {
	explicit ExchangeReadGlobalState(distributed::ExchangeSource source_p)
	    : source(std::move(source_p)), producer_count(NumericCast<idx_t>(source.producers_size())) {
	}

	idx_t MaxThreads() const override {
		return producer_count;
	}

	// Returns the next producer no thread has read yet, if any.
	optional_idx ClaimProducer() {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		if (next_producer == producer_count) {
			return optional_idx();
		}
		return next_producer++;
	}

	const distributed::ExchangeSource source;
	const idx_t producer_count;
	concurrency::mutex mu;
	idx_t next_producer DUCKDB_GUARDED_BY(mu) = 0;
};

struct ExchangeReadLocalState : public LocalTableFunctionState {
	~ExchangeReadLocalState() override {
		// A query that ends early, e.g. on a LIMIT or an error, stops the producer from sending the rest.
		if (stream) {
			stream->Cancel();
		}
	}

	// The client outlives its stream.
	unique_ptr<WorkerNodeClient> client;
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	unique_ptr<ArrowRecordBatchConverter> batch;
};

unique_ptr<FunctionData> ExchangeReadBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &,
                                          vector<Identifier> &) {
	throw BinderException("%s is only used in plans the driver sends to workers", EXCHANGE_READ_FUNCTION);
}

unique_ptr<GlobalTableFunctionState> ExchangeReadInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ExchangeReadBindData>();
	auto sources = context.registered_state->Get<ExchangeSources>(string(ExchangeSources::NAME));
	if (!sources || bind_data.exchange >= sources->sources.size()) {
		throw InvalidInputException("Task has no exchange source %llu", bind_data.exchange);
	}
	return make_uniq<ExchangeReadGlobalState>(sources->sources[bind_data.exchange]);
}

unique_ptr<LocalTableFunctionState> ExchangeReadInitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                          GlobalTableFunctionState *) {
	return make_uniq<ExchangeReadLocalState>();
}

void ExchangeReadFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ExchangeReadBindData>();
	auto &global = input.global_state->Cast<ExchangeReadGlobalState>();
	auto &local = input.local_state->Cast<ExchangeReadLocalState>();
	while (true) {
		if (local.batch && local.batch->Next(output)) {
			return;
		}
		local.batch.reset();
		if (local.stream) {
			auto next = local.stream->Next();
			if (!next.ok()) {
				throw IOException("Reading bucket %llu of exchange %s failed: %s", bind_data.bucket,
				                  global.source.exchange_id(), next.status().ToString());
			}
			if (next->data) {
				local.batch = make_uniq<ArrowRecordBatchConverter>(context, *next->data, &bind_data.types);
				continue;
			}
			local.stream.reset();
			local.client.reset();
		}
		auto producer = global.ClaimProducer();
		if (!producer.IsValid()) {
			output.SetChildCardinality(0);
			return;
		}
		const auto &location = global.source.producers(NumericCast<int>(producer.GetIndex()));
		local.client = make_uniq<WorkerNodeClient>(location);
		auto status = local.client->Connect();
		if (!status.ok()) {
			throw IOException("Connecting to exchange producer %s failed: %s", location, status.ToString());
		}
		auto stream = local.client->ReadExchange(global.source.exchange_id(), bind_data.bucket);
		if (!stream.ok()) {
			throw IOException("Reading bucket %llu of exchange %s from %s failed: %s", bind_data.bucket,
			                  global.source.exchange_id(), location, stream.status().ToString());
		}
		local.stream = std::move(stream).ValueOrDie();
	}
}

unique_ptr<NodeStatistics> ExchangeReadCardinality(ClientContext &, const FunctionData *bind_data) {
	auto cardinality = bind_data->Cast<ExchangeReadBindData>().estimated_cardinality;
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

void ExchangeReadSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                           const BoundTableFunction &) {
	auto &bind_data = bind_data_p->Cast<ExchangeReadBindData>();
	serializer.WriteProperty(100, "exchange", bind_data.exchange);
	serializer.WriteProperty(101, "bucket", bind_data.bucket);
	serializer.WriteProperty(102, "types", bind_data.types);
	serializer.WriteProperty(103, "estimated_cardinality", bind_data.estimated_cardinality);
}

unique_ptr<FunctionData> ExchangeReadDeserialize(Deserializer &deserializer, BoundTableFunction &) {
	auto bind_data = make_uniq<ExchangeReadBindData>();
	deserializer.ReadProperty(100, "exchange", bind_data->exchange);
	deserializer.ReadProperty(101, "bucket", bind_data->bucket);
	deserializer.ReadProperty(102, "types", bind_data->types);
	deserializer.ReadProperty(103, "estimated_cardinality", bind_data->estimated_cardinality);
	return std::move(bind_data);
}

TableFunction GetExchangeReadFunction() {
	TableFunction function(EXCHANGE_READ_FUNCTION, {}, ExchangeReadFunction, ExchangeReadBind, ExchangeReadInitGlobal,
	                       ExchangeReadInitLocal);
	function.cardinality = ExchangeReadCardinality;
	function.serialize = ExchangeReadSerialize;
	function.deserialize = ExchangeReadDeserialize;
	return function;
}

} // namespace

void RegisterExchangeRead(DatabaseInstance &db) {
	ExtensionLoader loader(db, "duckherder");
	loader.RegisterFunction(GetExchangeReadFunction());
}

unique_ptr<LogicalGet> MakeExchangeRead(idx_t table_index, idx_t exchange, idx_t bucket, vector<LogicalType> types,
                                        vector<string> names, idx_t estimated_cardinality) {
	auto bind_data = make_uniq<ExchangeReadBindData>();
	bind_data->exchange = exchange;
	bind_data->bucket = bucket;
	bind_data->types = types;
	bind_data->estimated_cardinality = estimated_cardinality;
	vector<Identifier> column_names(names.begin(), names.end());
	auto get = make_uniq<LogicalGet>(TableIndex(table_index), BoundTableFunction(GetExchangeReadFunction()),
	                                 std::move(bind_data), std::move(types), std::move(column_names));
	vector<ColumnIndex> column_ids;
	for (idx_t column_idx = 0; column_idx < get->returned_types.size(); ++column_idx) {
		column_ids.emplace_back(column_idx);
	}
	get->SetColumnIds(std::move(column_ids));
	get->estimated_cardinality = estimated_cardinality;
	get->has_estimated_cardinality = true;
	return get;
}

} // namespace duckdb
