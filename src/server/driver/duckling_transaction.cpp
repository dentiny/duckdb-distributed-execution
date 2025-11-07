#include "server/driver/duckling_transaction.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DucklingTransaction::DucklingTransaction(DuckTransactionManager &manager, ClientContext &context,
                                         transaction_t start_time, transaction_t transaction_id, idx_t catalog_version)
    : Transaction(manager, context),
      duckdb_transaction(make_uniq<DuckTransaction>(manager, context, start_time, transaction_id, catalog_version)) {
}

DucklingTransaction::~DucklingTransaction() = default;

} // namespace duckdb
