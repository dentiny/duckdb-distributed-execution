#include "duckherder_storage.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_transaction_manager.hpp"

#include <charconv>

namespace duckdb {

namespace {

struct RemoteEndpoint {
	string host;
	int port;
};

RemoteEndpoint ParseRemoteEndpoint(const string &path) {
	constexpr const char *GRPC_SCHEME = "grpc://";
	constexpr idx_t GRPC_SCHEME_LENGTH = 7;
	auto endpoint = path;
	if (StringUtil::StartsWith(endpoint, GRPC_SCHEME)) {
		endpoint = endpoint.substr(GRPC_SCHEME_LENGTH);
	} else if (endpoint.find("://") != string::npos) {
		throw InvalidInputException("Duckherder ATTACH only supports grpc:// endpoints");
	}

	idx_t port_separator;
	if (!endpoint.empty() && endpoint[0] == '[') {
		const auto closing_bracket = endpoint.find(']');
		if (closing_bracket == string::npos || closing_bracket + 1 >= endpoint.size() ||
		    endpoint[closing_bracket + 1] != ':') {
			throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected '[ipv6-address]:port'", path);
		}
		port_separator = closing_bracket + 1;
	} else {
		port_separator = endpoint.rfind(':');
		if (port_separator == string::npos || endpoint.find(':') != port_separator) {
			throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected 'host:port'", path);
		}
	}

	auto host = endpoint.substr(0, port_separator);
	const auto port_text = endpoint.substr(port_separator + 1);
	int port = 0;
	const auto parse_result = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
	if (host.empty() || port_text.empty() || parse_result.ec != std::errc() ||
	    parse_result.ptr != port_text.data() + port_text.size() || port < 1 || port > 65535) {
		throw InvalidInputException("Invalid Duckherder endpoint '%s'; expected 'host:port' with a valid port", path);
	}
	return {std::move(host), port};
}

unique_ptr<Catalog> DuckherderAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &options) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DuckherderAttach");

	if (options.options.find("server_host") != options.options.end() ||
	    options.options.find("server_port") != options.options.end()) {
		throw InvalidInputException(
		    "Duckherder server_host/server_port options are no longer supported; specify the endpoint in the "
		    "ATTACH path, for example ATTACH 'localhost:8815' (TYPE duckherder)");
	}
	auto endpoint = ParseRemoteEndpoint(info.path);
	const bool attach_read_only = options.access_mode == AccessMode::READ_ONLY;
	auto role = attach_read_only ? distributed::CLIENT_ROLE_READ_ONLY : distributed::CLIENT_ROLE_READ_WRITE;

	auto it = options.options.find("client_role");
	if (it != options.options.end()) {
		auto role_name = StringUtil::Upper(it->second.ToString());
		if (role_name == "READ_ONLY") {
			role = distributed::CLIENT_ROLE_READ_ONLY;
		} else if (role_name == "READ_WRITE") {
			if (attach_read_only) {
				throw InvalidInputException("Duckherder client_role 'read_write' conflicts with ATTACH READ_ONLY");
			}
			role = distributed::CLIENT_ROLE_READ_WRITE;
		} else {
			throw InvalidInputException("Duckherder client_role must be 'read_only' or 'read_write'");
		}
	}

	// Remove our custom options so StorageManager doesn't validate them.
	options.options.erase("client_role");

	// DuckCatalog is only the client-side metadata cache. Never persist its entries or table storage to the ATTACH
	// path. Its backing storage must remain writable even when the remote attachment itself is READ_ONLY.
	info.path = ":memory:";
	options.access_mode = AccessMode::READ_WRITE;

	auto catalog =
	    make_uniq<DuckherderCatalog>(db, std::move(endpoint.host), endpoint.port, role, context.GetConnectionId());
	catalog->GetClient(context);
	return std::move(catalog);
}

} // namespace

unique_ptr<TransactionManager> DuckherderCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	DUCKDB_LOG_DEBUG(db.GetDatabase(), "DuckherderCreateTransactionManager");
	return make_uniq<DuckherderTransactionManager>(db);
}

DuckherderStorageExtension::DuckherderStorageExtension() {
	attach = DuckherderAttach;
	create_transaction_manager = DuckherderCreateTransactionManager;
}

} // namespace duckdb
