#include "server/worker/exchange_read.hpp"

#include "arrow_record_batch_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "server/driver/worker_node_client.hpp"
#include "utils/mutex.hpp"

#include <arrow/flight/client.h>

namespace duckdb {

namespace {

struct ExchangeReadBindData : public TableFunctionData {
	string exchange_id;
	idx_t bucket = 0;
	vector<string> producers;
	vector<LogicalType> types;
	idx_t estimated_cardinality = 0;
};

// Each thread reads the bucket from one producer at a time.
struct ExchangeReadGlobalState : public GlobalTableFunctionState {
	explicit ExchangeReadGlobalState(idx_t producer_count_p) : producer_count(producer_count_p) {
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
                                          vector<string> &) {
	throw BinderException("%s is only used in plans the driver sends to workers", EXCHANGE_READ_FUNCTION);
}

unique_ptr<GlobalTableFunctionState> ExchangeReadInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ExchangeReadBindData>();
	return make_uniq<ExchangeReadGlobalState>(bind_data.producers.size());
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
				                  bind_data.exchange_id, next.status().ToString());
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
			output.SetCardinality(0);
			return;
		}
		auto &location = bind_data.producers[producer.GetIndex()];
		local.client = make_uniq<WorkerNodeClient>(location);
		auto status = local.client->Connect();
		if (!status.ok()) {
			throw IOException("Connecting to exchange producer %s failed: %s", location, status.ToString());
		}
		auto stream = local.client->ReadExchange(bind_data.exchange_id, bind_data.bucket);
		if (!stream.ok()) {
			throw IOException("Reading bucket %llu of exchange %s from %s failed: %s", bind_data.bucket,
			                  bind_data.exchange_id, location, stream.status().ToString());
		}
		local.stream = std::move(stream).ValueOrDie();
	}
}

unique_ptr<NodeStatistics> ExchangeReadCardinality(ClientContext &, const FunctionData *bind_data) {
	auto cardinality = bind_data->Cast<ExchangeReadBindData>().estimated_cardinality;
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

void ExchangeReadSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                           const TableFunction &) {
	auto &bind_data = bind_data_p->Cast<ExchangeReadBindData>();
	serializer.WriteProperty(100, "exchange_id", bind_data.exchange_id);
	serializer.WriteProperty(101, "bucket", bind_data.bucket);
	serializer.WriteProperty(102, "producers", bind_data.producers);
	serializer.WriteProperty(103, "types", bind_data.types);
	serializer.WriteProperty(104, "estimated_cardinality", bind_data.estimated_cardinality);
}

unique_ptr<FunctionData> ExchangeReadDeserialize(Deserializer &deserializer, TableFunction &) {
	auto bind_data = make_uniq<ExchangeReadBindData>();
	deserializer.ReadProperty(100, "exchange_id", bind_data->exchange_id);
	deserializer.ReadProperty(101, "bucket", bind_data->bucket);
	deserializer.ReadProperty(102, "producers", bind_data->producers);
	deserializer.ReadProperty(103, "types", bind_data->types);
	deserializer.ReadProperty(104, "estimated_cardinality", bind_data->estimated_cardinality);
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

unique_ptr<LogicalGet> MakeExchangeRead(idx_t table_index, string exchange_id, idx_t bucket, vector<string> producers,
                                        vector<LogicalType> types, vector<string> names, idx_t estimated_cardinality) {
	auto bind_data = make_uniq<ExchangeReadBindData>();
	bind_data->exchange_id = std::move(exchange_id);
	bind_data->bucket = bucket;
	bind_data->producers = std::move(producers);
	bind_data->types = types;
	bind_data->estimated_cardinality = estimated_cardinality;
	auto get = make_uniq<LogicalGet>(table_index, GetExchangeReadFunction(), std::move(bind_data), std::move(types),
	                                 std::move(names));
	for (idx_t column_idx = 0; column_idx < get->returned_types.size(); ++column_idx) {
		get->AddColumnId(column_idx);
	}
	get->estimated_cardinality = estimated_cardinality;
	get->has_estimated_cardinality = true;
	return get;
}

} // namespace duckdb
