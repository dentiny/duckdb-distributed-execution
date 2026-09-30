#include "client/execution/logical_remote_create_index.hpp"

#include "client/execution/distributed_create_index.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

class RemoteCreateIndexOperatorExtension : public OperatorExtension {
public:
	RemoteCreateIndexOperatorExtension() {
		Bind = [](ClientContext &, Binder &, OperatorExtensionInfo *, SQLStatement &) {
			return BoundStatement();
		};
	}

	string GetName() override {
		return "duckherder_remote_create_index";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		auto create_info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(201, "create_info");
		auto catalog_name = deserializer.ReadProperty<string>(202, "catalog_name");
		auto schema_name = deserializer.ReadProperty<string>(203, "schema_name");
		auto table_name = deserializer.ReadProperty<string>(204, "table_name");

		auto &context = deserializer.Get<ClientContext &>();
		auto &catalog = Catalog::GetCatalog(context, catalog_name);
		auto &schema = catalog.GetSchema(context, schema_name);
		auto &entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema_name, table_name);
		auto &table = entry.Cast<TableCatalogEntry>();
		auto info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(create_info));
		return make_uniq<LogicalRemoteCreateIndexOperator>(std::move(info), schema, table);
	}
};

} // namespace

LogicalRemoteCreateIndexOperator::LogicalRemoteCreateIndexOperator(unique_ptr<CreateIndexInfo> info_p,
                                                                   SchemaCatalogEntry &schema_p,
                                                                   TableCatalogEntry &table_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), schema(schema_p), table(table_p) {
}

PhysicalOperator &LogicalRemoteCreateIndexOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Create the physical operator for remote CREATE INDEX.
	// Make a copy of the info since the system might need to access the logical operator later.
	auto info_copy = unique_ptr_cast<CreateInfo, CreateIndexInfo>(info->Copy());

	// Pass the catalog, schema, and table names instead of references to avoid dangling reference issues.
	string catalog_name = schema.catalog.GetName();
	string schema_name = schema.name;
	string table_name = table.name;

	return planner.Make<PhysicalRemoteCreateIndexOperator>(std::move(info_copy), std::move(catalog_name),
	                                                       std::move(schema_name), std::move(table_name),
	                                                       estimated_cardinality);
}

void LogicalRemoteCreateIndexOperator::Serialize(Serializer &serializer) const {
	LogicalExtensionOperator::Serialize(serializer);
	serializer.WriteProperty(201, "create_info", info);
	serializer.WriteProperty(202, "catalog_name", schema.catalog.GetName());
	serializer.WriteProperty(203, "schema_name", schema.name);
	serializer.WriteProperty(204, "table_name", table.name);
}

shared_ptr<OperatorExtension> GetRemoteCreateIndexOperatorExtension() {
	return make_shared_ptr<RemoteCreateIndexOperatorExtension>();
}

} // namespace duckdb
