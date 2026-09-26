#include "duckherder_transaction.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DuckherderTransaction::DuckherderTransaction(DuckTransactionManager &manager, ClientContext &context,
                                             transaction_t start_time, SnapshotView view, idx_t catalog_version)
    : Transaction(manager, context),
      duckdb_transaction(make_uniq<DuckTransaction>(manager, context, start_time, view, catalog_version)) {
}

DuckherderTransaction::~DuckherderTransaction() = default;

} // namespace duckdb
