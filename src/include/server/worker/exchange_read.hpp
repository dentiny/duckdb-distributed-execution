#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "worker.pb.h"

#include <string_view>

namespace duckdb {

class DatabaseInstance;
class LogicalGet;

// Name of the table function that reads one bucket of an exchange from every producer worker.
constexpr const char *EXCHANGE_READ_FUNCTION = "duckherder_exchange_read";

// The exchanges that a worker task's exchange reads scan, set from the task's request, so the driver picks exchange ids
// and producers when it runs the plan, not when it plans it.
class ExchangeSources : public ClientContextState {
public:
	static constexpr std::string_view NAME = "duckherder_exchange_sources";

	explicit ExchangeSources(vector<distributed::ExchangeSource> sources_p) : sources(std::move(sources_p)) {
	}

	vector<distributed::ExchangeSource> sources;
};

// Registers the exchange read function, so plans that use it can be deserialized in db.
void RegisterExchangeRead(DatabaseInstance &db);

// Returns a scan of bucket of the task's exchange source number exchange, with the given columns. The driver puts it in
// consumer task plans; it cannot be written in SQL.
unique_ptr<LogicalGet> MakeExchangeRead(idx_t table_index, idx_t exchange, idx_t bucket, vector<LogicalType> types,
                                        vector<string> names, idx_t estimated_cardinality);

} // namespace duckdb
