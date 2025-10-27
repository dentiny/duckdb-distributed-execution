#include "motherduck_pragmas.hpp"
#include "motherduck_catalog.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

/*static*/ void MotherduckPragmas::RegisterPragmas(ExtensionLoader &loader) {
	// Register pragma function for registering remote tables.
	// Takes 3 positional arguments: table_name, server_url, remote_table_name
	auto register_function =
	    PragmaFunction::PragmaCall("md_register_remote_table", RegisterRemoteTable,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(register_function);

	// Register pragma function for unregistering remote tables.
	// Takes 1 argument: table_name
	auto unregister_function =
	    PragmaFunction::PragmaCall("md_unregister_remote_table", UnregisterRemoteTable, {LogicalType::VARCHAR});
	loader.RegisterFunction(unregister_function);
}

/*static*/ void MotherduckPragmas::RegisterRemoteTable(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto server_url = parameters.values[1].ToString();
	auto remote_table_name = parameters.values[2].ToString();

	std::cout << "🔧 PRAGMA: Registering '" << table_name << "' -> '" << server_url << "'/'" << remote_table_name << "'" << std::endl;

	// Get the motherduck catalog - assuming it's attached as "md".
	auto &db_manager = DatabaseManager::Get(context);
	auto md_db = db_manager.GetDatabase(context, "md");
	if (!md_db) {
		throw Exception(ExceptionType::CATALOG, "Motherduck database 'md' not attached");
	}

	auto &catalog = md_db->GetCatalog();
	std::cout << "   Catalog type: " << catalog.GetCatalogType() << ", name: " << catalog.GetName() << std::endl;
	
	if (catalog.GetCatalogType() != "motherduck") {
		throw Exception(ExceptionType::CATALOG, "Database 'md' is not a motherduck database");
	}

	auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&catalog);
	if (!md_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to MotherduckCatalog");
	}
	
	md_catalog_ptr->RegisterRemoteTable(table_name, server_url, remote_table_name);
	std::cout << "   ✅ Registered successfully! Catalog now has remote table." << std::endl;
}

/*static*/ void MotherduckPragmas::UnregisterRemoteTable(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();

	// Get the motherduck catalog - assuming it's attached as "md".
	auto &db_manager = DatabaseManager::Get(context);
	auto md_db = db_manager.GetDatabase(context, "md");
	if (!md_db) {
		throw Exception(ExceptionType::CATALOG, "Motherduck database 'md' not attached");
	}

	auto &catalog = md_db->GetCatalog();
	if (catalog.GetCatalogType() != "motherduck") {
		throw Exception(ExceptionType::CATALOG, "Database 'md' is not a motherduck database");
	}

	auto md_catalog_ptr = dynamic_cast<MotherduckCatalog *>(&catalog);
	if (!md_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to MotherduckCatalog");
	}
	md_catalog_ptr->UnregisterRemoteTable(table_name);
}

} // namespace duckdb
