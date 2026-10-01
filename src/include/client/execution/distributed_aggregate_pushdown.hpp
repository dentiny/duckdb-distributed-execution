#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Replaces aggregates over remote table scans with a single remote scan whose query computes the aggregate on the
// server, so only aggregated rows are transferred to the client. Plans that cannot be translated safely are kept.
OptimizerExtension GetDistributedAggregatePushdownExtension();

} // namespace duckdb
