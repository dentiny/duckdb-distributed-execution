#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

// Logical operator for CREATE INDEX on remote tables.
// This bypasses the normal index binding flow since we don't need to scan the table locally.
class LogicalRemoteCreateIndexOperator : public LogicalExtensionOperator {
public:
	LogicalRemoteCreateIndexOperator(unique_ptr<CreateIndexInfo> info_p, SchemaCatalogEntry &schema_p,
	                                 TableCatalogEntry &table_p);

	unique_ptr<CreateIndexInfo> info;
	SchemaCatalogEntry &schema;
	TableCatalogEntry &table;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	string GetExtensionName() const override {
		return "duckherder_remote_create_index";
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType {LogicalTypeId::BIGINT});
	}
};

} // namespace duckdb
