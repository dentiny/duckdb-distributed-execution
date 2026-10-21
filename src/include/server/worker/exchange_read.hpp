#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DatabaseInstance;
class LogicalGet;

// Name of the table function that reads one bucket of an exchange from every producer worker.
constexpr const char *EXCHANGE_READ_FUNCTION = "duckherder_exchange_read";

// Registers the exchange read function, so plans that use it can be deserialized in db.
void RegisterExchangeRead(DatabaseInstance &db);

// Returns a scan of bucket of the exchange, whose rows have the given columns, written by producer tasks on the workers
// at producers (Flight locations). The driver puts it in consumer task plans; it cannot be written in SQL.
unique_ptr<LogicalGet> MakeExchangeRead(idx_t table_index, string exchange_id, idx_t bucket, vector<string> producers,
                                        vector<LogicalType> types, vector<string> names, idx_t estimated_cardinality);

} // namespace duckdb
