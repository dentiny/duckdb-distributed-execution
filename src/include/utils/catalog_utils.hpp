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

// Generate SQL statement to alter table.
string GenerateAlterTableSQL(AlterTableInfo &info, const string &table_name);

// Get the DistributedClient from a TableCatalogEntry's parent catalog.
DistributedClient &GetDistributedClient(ClientContext &context, TableCatalogEntry &table);

// Get the DistributedClient from a Catalog.
DistributedClient &GetDistributedClient(ClientContext &context, Catalog &catalog);

// Return the quoted `schema.entry` name of a catalog entry on the control node.
string QualifiedRemoteName(const string &schema_name, const string &entry_name);

} // namespace duckdb
