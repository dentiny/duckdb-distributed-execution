#include "duckherder_pragmas.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckherder_catalog.hpp"

namespace duckdb {

/*static*/ void DuckherderPragmas::RegisterPragmas(ExtensionLoader &loader) {
	// Register pragma function for registering remote tables.
	// Takes 3 positional arguments: table_name, server_url, remote_table_name
	auto register_function =
	    PragmaFunction::PragmaCall("dh_register_remote_table", RegisterRemoteTable,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(register_function);

	// Register pragma function for unregistering remote tables.
	// Takes 1 argument: table_name
	auto unregister_function =
	    PragmaFunction::PragmaCall("dh_unregister_remote_table", UnregisterRemoteTable, {LogicalType::VARCHAR});
	loader.RegisterFunction(unregister_function);
}

/*static*/ void DuckherderPragmas::RegisterRemoteTable(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto server_url = parameters.values[1].ToString();
	auto remote_table_name = parameters.values[2].ToString();

	// Get the duckherder catalog - assuming it's attached as "dh".
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw Exception(ExceptionType::CATALOG, "Duckherder database 'dh' not attached");
	}

	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw Exception(ExceptionType::CATALOG, "Database 'md' is not a duckherder database");
	}

	auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
	if (!dh_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to DuckherderCatalog");
	}

	dh_catalog_ptr->RegisterRemoteTable(table_name, server_url, remote_table_name);
}

/*static*/ void DuckherderPragmas::UnregisterRemoteTable(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();

	// Get the duckherder catalog - assuming it's attached as "dh".
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw Exception(ExceptionType::CATALOG, "Duckherder database 'dh' not attached");
	}

	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw Exception(ExceptionType::CATALOG, "Database 'md' is not a duckherder database");
	}

	auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
	if (!dh_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to DuckherderCatalog");
	}
	dh_catalog_ptr->UnregisterRemoteTable(table_name);
}

} // namespace duckdb
