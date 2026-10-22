#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

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
	struct PendingTable {
		unique_ptr<CreateTableInfo> info;
		optional_idx estimated_size;
		bool loaded = false;
	};

	void LoadSchema(const Identifier &schema_name);
	void LoadEnumType(DuckherderSchemaCatalogEntry &schema, const Identifier &type_name, const Value &labels);
	void LoadTable(const Identifier &schema_name, const Identifier &table_name, PendingTable &table);

	DuckherderCatalog &catalog;
	ClientContext &context;
	CatalogTransaction transaction;
	// Schema name -> table name -> table.
	identifier_map_t<identifier_map_t<PendingTable>> pending_tables;
};

} // namespace duckdb
