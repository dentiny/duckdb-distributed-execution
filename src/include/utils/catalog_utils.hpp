#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Forward declaration.
struct AlterTableInfo;
class Catalog;
class ClientContext;
class TableCatalogEntry;
class DistributedClient;

// Return the complete statement currently being planned. SQL PREPARE wraps the
// actual DML statement, so unwrap it before storing SQL in a remote operator.
string GetRemoteStatementSQL(ClientContext &context);

// Util function to sanitize query and remove all occurrences of the catalog prefix from SQL string.
string SanitizeQuery(const string &sql, const string &catalog_name);

// Generate SQL statement to alter table.
string GenerateAlterTableSQL(AlterTableInfo &info, const string &table_name);

// Get the DistributedClient from a TableCatalogEntry's parent catalog.
DistributedClient &GetDistributedClient(TableCatalogEntry &table);

// Get the DistributedClient from a Catalog.
DistributedClient &GetDistributedClient(Catalog &catalog);

} // namespace duckdb
