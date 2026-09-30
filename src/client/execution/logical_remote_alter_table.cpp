#include "client/execution/logical_remote_alter_table.hpp"

#include "client/execution/distributed_alter_table.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

class RemoteAlterTableOperatorExtension : public OperatorExtension {
public:
	RemoteAlterTableOperatorExtension() {
		Bind = [](ClientContext &, Binder &, OperatorExtensionInfo *, SQLStatement &) {
			return BoundStatement();
		};
	}

	string GetName() override {
		return "duckherder_remote_alter_table";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		auto parse_info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(201, "alter_info");
		auto catalog_name = deserializer.ReadProperty<string>(202, "catalog_name");
		auto schema_name = deserializer.ReadProperty<string>(203, "schema_name");
		auto table_name = deserializer.ReadProperty<string>(204, "table_name");

		auto &context = deserializer.Get<ClientContext &>();
		auto &catalog = Catalog::GetCatalog(context, catalog_name);
		auto &schema = catalog.GetSchema(context, schema_name);
		auto &entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema_name, table_name);
		auto &table = entry.Cast<TableCatalogEntry>();
		auto alter_info = unique_ptr_cast<ParseInfo, AlterInfo>(std::move(parse_info));
		auto info = unique_ptr_cast<AlterInfo, AlterTableInfo>(std::move(alter_info));
		return make_uniq<LogicalRemoteAlterTableOperator>(std::move(info), schema, table);
	}
};

} // namespace

LogicalRemoteAlterTableOperator::LogicalRemoteAlterTableOperator(unique_ptr<AlterTableInfo> info_p,
                                                                 SchemaCatalogEntry &schema_p,
                                                                 TableCatalogEntry &table_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), schema(schema_p), table(table_p) {
}

PhysicalOperator &LogicalRemoteAlterTableOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Create the physical operator for remote ALTER TABLE.
	// Make a copy of the info since the system might need to access the logical operator later.
	auto info_copy = unique_ptr_cast<AlterInfo, AlterTableInfo>(info->Copy());

	// Pass the catalog, schema, and table names instead of references to avoid dangling reference issues.
	string catalog_name = schema.catalog.GetName();
	string schema_name = schema.name;
	string table_name = table.name;

	return planner.Make<PhysicalRemoteAlterTableOperator>(std::move(info_copy), std::move(catalog_name),
	                                                      std::move(schema_name), std::move(table_name),
	                                                      estimated_cardinality);
}

void LogicalRemoteAlterTableOperator::Serialize(Serializer &serializer) const {
	LogicalExtensionOperator::Serialize(serializer);
	serializer.WriteProperty(201, "alter_info", info);
	serializer.WriteProperty(202, "catalog_name", schema.catalog.GetName());
	serializer.WriteProperty(203, "schema_name", schema.name);
	serializer.WriteProperty(204, "table_name", table.name);
}

shared_ptr<OperatorExtension> GetRemoteAlterTableOperatorExtension() {
	return make_shared_ptr<RemoteAlterTableOperatorExtension>();
}

} // namespace duckdb
