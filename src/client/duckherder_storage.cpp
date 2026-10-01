#include "duckherder_storage.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_remote_endpoint.hpp"
#include "duckherder_transaction_manager.hpp"

namespace duckdb {

namespace {

unique_ptr<SecretEntry> FindS3Secret(ClientContext &context, optional_ptr<const Value> secret_name,
                                     const string &scope) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (!secret_name) {
		auto match = secret_manager.LookupSecret(transaction, scope, "s3");
		return match.HasMatch() ? std::move(match.secret_entry) : nullptr;
	}
	if (secret_name->IsNull()) {
		throw InvalidInputException("Duckherder SECRET cannot be NULL");
	}
	auto name = secret_name->GetValue<string>();
	if (name.empty()) {
		throw InvalidInputException("Duckherder SECRET cannot be empty");
	}
	auto secret = secret_manager.GetSecretByName(transaction, name);
	if (!secret) {
		throw InvalidInputException("Secret with name \"%s\" not found", name);
	}
	if (secret->secret->GetType() != "s3") {
		throw InvalidInputException("Secret \"%s\" has type \"%s\"; Duckherder S3 storage requires TYPE S3", name,
		                            secret->secret->GetType());
	}
	return secret;
}

void ApplyS3Secret(const SecretEntry &entry, distributed::S3Storage &config) {
	auto secret = dynamic_cast<const KeyValueSecret *>(entry.secret.get());
	if (!secret) {
		throw InvalidInputException("Duckherder S3 storage requires a key-value S3 secret");
	}
	auto copy_string = [&](const string &key, auto setter) {
		Value value;
		if (secret->TryGetValue(key, value) && !value.IsNull()) {
			setter(value.ToString());
		}
	};
	copy_string("key_id", [&](const string &value) { config.set_key_id(value); });
	copy_string("secret", [&](const string &value) { config.set_secret(value); });
	copy_string("session_token", [&](const string &value) { config.set_session_token(value); });
	copy_string("endpoint", [&](const string &value) { config.set_endpoint(value); });
	copy_string("region", [&](const string &value) { config.set_region(value); });

	Value use_ssl;
	if (secret->TryGetValue("use_ssl", use_ssl) && !use_ssl.IsNull()) {
		config.set_use_ssl(use_ssl.GetValue<bool>());
	}
	Value url_style;
	if (secret->TryGetValue("url_style", url_style) && !url_style.IsNull()) {
		auto style = StringUtil::Lower(url_style.ToString());
		if (style == "path") {
			config.set_url_style(distributed::S3_URL_STYLE_PATH);
		} else if (style == "vhost") {
			config.set_url_style(distributed::S3_URL_STYLE_VHOST);
		} else {
			throw InvalidInputException("S3 secret url_style must be either 'path' or 'vhost'");
		}
	}
}

void ConfigureS3Storage(ClientContext &context, const string &data_path, optional_ptr<const Value> secret_name,
                        distributed::StorageConfig &config) {
	auto path = data_path.substr(string("s3://").size());
	auto separator = path.find('/');
	auto bucket = separator == string::npos ? path : path.substr(0, separator);
	if (bucket.empty()) {
		throw InvalidInputException("Duckherder S3 DATA_PATH must include a bucket");
	}
	auto &s3 = *config.mutable_s3();
	s3.set_bucket(bucket);
	if (separator != string::npos) {
		s3.set_root(path.substr(separator + 1));
	}
	auto secret = FindS3Secret(context, secret_name, data_path);
	if (secret) {
		ApplyS3Secret(*secret, s3);
	}
	if (s3.key_id().empty() != s3.secret().empty()) {
		throw InvalidInputException("S3 KEY_ID and SECRET must be provided together");
	}
	if (!s3.session_token().empty() && s3.key_id().empty()) {
		throw InvalidInputException("S3 SESSION_TOKEN requires KEY_ID and SECRET");
	}
}

// Build the object storage selection from the endpoint's database name and DATA_PATH, removing the option so
// StorageManager doesn't validate it.
distributed::StorageConfig ExtractStorageConfig(ClientContext &context, const string &database_name,
                                                AttachOptions &options) {
	optional_ptr<const Value> secret_name;
	auto secret_entry = options.options.find("secret");
	if (secret_entry != options.options.end()) {
		secret_name = &secret_entry->second;
	}
	string data_path;
	auto entry = options.options.find("data_path");
	if (entry != options.options.end()) {
		data_path = entry->second.ToString();
		options.options.erase(entry);
	}

	distributed::StorageConfig config;
	if (database_name.empty()) {
		if (!data_path.empty() || secret_name) {
			throw InvalidInputException("Duckherder DATA_PATH requires a database name, for example "
			                            "ATTACH 'localhost:8815/db_name' (TYPE duckherder, DATA_PATH '/path')");
		}
		return config;
	}
	config.set_database_uri(StringUtil::Format("duckdb_objfs://%s", database_name));
	if (data_path.empty()) {
		if (secret_name) {
			throw InvalidInputException("Duckherder SECRET requires an S3 DATA_PATH");
		}
		config.mutable_in_memory();
	} else if (StringUtil::StartsWith(data_path, "s3://")) {
		ConfigureS3Storage(context, data_path, secret_name, config);
	} else if (data_path.find("://") != string::npos) {
		throw NotImplementedException("Duckherder DATA_PATH supports local paths and s3:// URIs, got '%s'", data_path);
	} else {
		if (secret_name) {
			throw InvalidInputException("Duckherder SECRET requires an S3 DATA_PATH");
		}
		config.mutable_local()->set_root(data_path);
	}
	options.options.erase("secret");
	return config;
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
	auto storage_config = ExtractStorageConfig(context, endpoint.database_name, options);

	// DuckCatalog is only the client-side metadata cache. Never persist its entries or table storage to the ATTACH
	// path. Its backing storage must remain writable even when the remote attachment itself is READ_ONLY.
	info.path = ":memory:";
	options.access_mode = AccessMode::READ_WRITE;

	auto catalog = make_uniq<DuckherderCatalog>(db, std::move(endpoint.host), endpoint.port, role,
	                                            context.GetConnectionId(), std::move(storage_config));
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
