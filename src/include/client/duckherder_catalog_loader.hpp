#pragma once

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ClientContext;
class DuckherderCatalog;
class DuckherderSchemaCatalogEntry;
class Value;

// Loads the control node's schemas, enum types, and tables into a Duckherder catalog's local metadata cache.
class DuckherderCatalogLoader {
public:
	DuckherderCatalogLoader(DuckherderCatalog &catalog, ClientContext &context);

	void Load();

private:
	void LoadSchema(const string &schema_name);
	void LoadEnumType(DuckherderSchemaCatalogEntry &schema, const string &type_name, const Value &labels);
	void LoadTable(DuckherderSchemaCatalogEntry &schema, const string &table_name, const string &sql,
	               const Value &estimated_size);

	DuckherderCatalog &catalog;
	ClientContext &context;
	CatalogTransaction transaction;
};

} // namespace duckdb
