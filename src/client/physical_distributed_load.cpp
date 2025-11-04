#include "physical_distributed_load.hpp"

#include "distributed_flight_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckherder_catalog.hpp"

namespace duckdb {

static void InstallFromRepository(ClientContext &context, const LoadInfo &info) {
	ExtensionRepository repository;
	if (!info.repository.empty() && info.repo_is_alias) {
		auto repository_url = ExtensionRepository::TryGetRepositoryUrl(info.repository);
		// This has been checked during bind, so it should not fail here
		if (repository_url.empty()) {
			throw InternalException("The repository alias failed to resolve");
		}
		repository = ExtensionRepository(info.repository, repository_url);
	} else if (!info.repository.empty()) {
		repository = ExtensionRepository::GetRepositoryByUrl(info.repository);
	}

	ExtensionInstallOptions options;
	options.force_install = info.load_type == LoadType::FORCE_INSTALL;
	options.throw_on_origin_mismatch = true;
	options.version = info.version;
	options.repository = repository;

	ExtensionHelper::InstallExtension(context, info.filename, options);
}

SourceResultType PhysicalDistributedLoad::GetData(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	// First, load locally
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		if (info->repository.empty()) {
			ExtensionInstallOptions options;
			options.force_install = info->load_type == LoadType::FORCE_INSTALL;
			options.throw_on_origin_mismatch = true;
			options.version = info->version;
			ExtensionHelper::InstallExtension(context.client, info->filename, options);
		} else {
			InstallFromRepository(context.client, *info);
		}
	} else {
		ExtensionHelper::LoadExternalExtension(context.client, info->filename);
	}

	// Then, forward to server
	try {
		auto server_url = catalog.GetServerUrl();
		DistributedFlightClient client(server_url);
		auto status = client.Connect();
		if (!status.ok()) {
			DUCKDB_LOG_DEBUG(*context.client.db,
			                 StringUtil::Format("Failed to connect to server: %s", status.ToString()));
			// Don't fail the entire operation if server connection fails
			return SourceResultType::FINISHED;
		}

		distributed::DistributedResponse response;
		status = client.LoadExtension(info->filename, info->repository, info->version, response);

		if (!status.ok()) {
			DUCKDB_LOG_DEBUG(*context.client.db,
			                 StringUtil::Format("Failed to load extension on server: %s", status.ToString()));
		} else if (!response.success()) {
			DUCKDB_LOG_DEBUG(*context.client.db,
			                 StringUtil::Format("Server failed to load extension: %s", response.error_message()));
		}
	} catch (std::exception &ex) {
		DUCKDB_LOG_DEBUG(*context.client.db,
		                 StringUtil::Format("Exception while loading extension on server: %s", ex.what()));
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb

