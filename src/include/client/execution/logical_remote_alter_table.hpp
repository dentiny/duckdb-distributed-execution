#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

// Logical operator for ALTER TABLE on remote tables.
// This handles remote table alterations without requiring local table scanning.
class LogicalRemoteAlterTableOperator : public LogicalExtensionOperator {
public:
	LogicalRemoteAlterTableOperator(unique_ptr<AlterTableInfo> info_p, SchemaCatalogEntry &schema_p,
	                                TableCatalogEntry &table_p);

	unique_ptr<AlterTableInfo> info;
	SchemaCatalogEntry &schema;
	TableCatalogEntry &table;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	string GetExtensionName() const override {
		return "duckherder_remote_alter_table";
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType {LogicalTypeId::BIGINT});
	}
};

} // namespace duckdb
