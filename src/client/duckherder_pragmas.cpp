#include "duckherder_pragmas.hpp"

#include "distributed_flight_client.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckherder_catalog.hpp"

namespace duckdb {

/*static*/ void DuckherderPragmas::RegisterPragmas(ExtensionLoader &loader) {
	// Register pragma function for registering remote tables.
	// Takes 2 positional arguments: table_name, remote_table_name
	auto register_function =
	    PragmaFunction::PragmaCall("duckherder_register_remote_table", RegisterRemoteTable,
	                               {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}});
	loader.RegisterFunction(register_function);

	// Register pragma function for unregistering remote tables.
	// Takes 1 argument: table_name
	auto unregister_function = PragmaFunction::PragmaCall("duckherder_unregister_remote_table", UnregisterRemoteTable,
	                                                      {LogicalType {LogicalTypeId::VARCHAR}});
	loader.RegisterFunction(unregister_function);

	// Register scalar function for loading extensions on server.
	// Takes 1 argument: extension_name
	ScalarFunction load_extension_func(
	    "duckherder_load_extension",
	    {LogicalType::VARCHAR},
	    LogicalType::BOOLEAN,
	    LoadExtensionScalar
	);
	loader.RegisterFunction(load_extension_func);
}

/*static*/ void DuckherderPragmas::RegisterRemoteTable(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto remote_table_name = parameters.values[1].ToString();

	// Get the duckherder catalog - assuming it's attached as "dh".
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw Exception(ExceptionType::CATALOG, "Duckherder database 'dh' not attached");
	}

	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw Exception(ExceptionType::CATALOG, "Database 'dh' is not a duckherder database");
	}

	auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
	if (!dh_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to DuckherderCatalog");
	}

	auto server_url = dh_catalog_ptr->GetServerUrl();
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
		throw Exception(ExceptionType::CATALOG, "Database 'dh' is not a duckherder database");
	}

	auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
	if (!dh_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to DuckherderCatalog");
	}
	dh_catalog_ptr->UnregisterRemoteTable(table_name);
}

/*static*/ void DuckherderPragmas::LoadExtension(ClientContext &context, const FunctionParameters &parameters) {
	auto extension_name = parameters.values[0].ToString();

	// Get the duckherder catalog - assuming it's attached as "dh".
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw Exception(ExceptionType::CATALOG, "Duckherder database 'dh' not attached");
	}

	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw Exception(ExceptionType::CATALOG, "Database 'dh' is not a duckherder database");
	}

	auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
	if (!dh_catalog_ptr) {
		throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to DuckherderCatalog");
	}

	// Connect to server and load extension.
	auto server_url = dh_catalog_ptr->GetServerUrl();
	DistributedFlightClient client(server_url);
	
	auto status = client.Connect();
	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, StringUtil::Format("Failed to connect to server: %s", status.ToString()));
	}

	distributed::DistributedResponse response;
	status = client.LoadExtension(extension_name, "", "", response);

	if (!status.ok()) {
		throw Exception(ExceptionType::CONNECTION, StringUtil::Format("Failed to load extension on server: %s", status.ToString()));
	}
	if (!response.success()) {
		throw Exception(ExceptionType::EXECUTOR, StringUtil::Format("Server failed to load extension: %s", response.error_message()));
	}
}

/*static*/ void DuckherderPragmas::LoadExtensionScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &extension_name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(
	    extension_name_vector, result, args.size(),
	    [&](string_t extension_name) {
		    auto extension_name_str = extension_name.GetString();
		    auto &context = state.GetContext();

			// Load extension on server side.
			//
		    // Get the duckherder catalog - assuming it's attached as "dh".
		    auto &db_manager = DatabaseManager::Get(context);
		    auto dh_db = db_manager.GetDatabase(context, "dh");
		    if (!dh_db) {
			    throw Exception(ExceptionType::CATALOG, "Duckherder database 'dh' not attached");
		    }

		    auto &catalog = dh_db->GetCatalog();
		    if (catalog.GetCatalogType() != "duckherder") {
			    throw Exception(ExceptionType::CATALOG, "Database 'dh' is not a duckherder database");
		    }

		    auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
		    if (!dh_catalog_ptr) {
			    throw Exception(ExceptionType::CATALOG, "Failed to cast catalog to DuckherderCatalog");
		    }

		    auto server_url = dh_catalog_ptr->GetServerUrl();
		    DistributedFlightClient client(server_url);
		    
		    auto status = client.Connect();
		    if (!status.ok()) {
			    throw Exception(ExceptionType::CONNECTION, 
			                    StringUtil::Format("Failed to connect to server %s because %s", server_url, status.ToString()));
		    }

		    distributed::DistributedResponse response;
		    status = client.LoadExtension(extension_name_str, "", "", response);
		    if (!status.ok()) {
			    throw Exception(ExceptionType::CONNECTION, 
			                    StringUtil::Format("Failed to load extension on server %s: %s", extension_name_str, status.ToString()));
		    }
		    if (!response.success()) {
			    throw Exception(ExceptionType::EXECUTOR, 
			                    StringUtil::Format("Server failed to load extension %s: %s", extension_name_str, response.error_message()));
		    }
		    
		    // Attempt to load extension on client side to keep compatibility.
		    try {
			    ExtensionHelper::LoadExternalExtension(context, extension_name_str);
		    } catch (std::exception &ex) {
			    auto &db = DatabaseInstance::GetDatabase(context);
			    DUCKDB_LOG_DEBUG(db, StringUtil::Format("Failed to load extension %s because %s", extension_name_str, ex.what()));
		    }
		    
		    return true;
	    });
}

} // namespace duckdb
