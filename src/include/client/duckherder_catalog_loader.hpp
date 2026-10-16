#pragma once

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class ClientContext;
class DuckherderCatalog;
class DuckherderSchemaCatalogEntry;

// Loads the control node's schemas, enum types, and tables into a Duckherder catalog's local metadata cache.
class DuckherderCatalogLoader {
public:
	DuckherderCatalogLoader(DuckherderCatalog &catalog, ClientContext &context);

	void Load();

private:
	struct PendingTable {
		unique_ptr<CreateTableInfo> info;
		Value estimated_size;
		bool loaded = false;
	};

	void LoadSchema(const string &schema_name);
	void LoadEnumType(DuckherderSchemaCatalogEntry &schema, const string &type_name, const Value &labels);
	void LoadTable(const string &schema_name, const string &table_name, PendingTable &table);

	DuckherderCatalog &catalog;
	ClientContext &context;
	CatalogTransaction transaction;
	// Schema name -> table name -> table.
	case_insensitive_map_t<case_insensitive_map_t<PendingTable>> pending_tables;
};

} // namespace duckdb
