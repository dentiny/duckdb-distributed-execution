#include "client/duckherder_catalog_loader.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_schema_catalog_entry.hpp"
#include "query_common.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

unique_ptr<CreateInfo> ParseCreateInfo(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw CatalogException("Expected a single CREATE statement in remote catalog metadata: %s", sql);
	}
	return std::move(parser.statements[0]->Cast<CreateStatement>().info);
}

template <class FUNC>
void ForEachRow(DistributedClient &client, const string &sql, const vector<LogicalType> &types, FUNC &&callback) {
	auto result = client.ScanTable(sql, NO_QUERY_LIMIT, NO_QUERY_OFFSET, &types);
	if (result->HasError()) {
		throw IOException("Failed to load remote catalog metadata: %s", result->GetError());
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			callback(*chunk, row_idx);
		}
	}
}

} // namespace

DuckherderCatalogLoader::DuckherderCatalogLoader(DuckherderCatalog &catalog_p, ClientContext &context_p)
    : catalog(catalog_p), context(context_p),
      transaction(CatalogTransaction::GetSystemTransaction(catalog_p.db_instance)) {
}

void DuckherderCatalogLoader::Load() {
	auto &client = catalog.GetClient(context);

	// Fetch one ordered snapshot so concurrent remote DDL cannot leave a partially discovered catalog.
	ForEachRow(client,
	           "SELECT 0 AS entry_order, schema_name, NULL::VARCHAR AS entry_name, NULL::VARCHAR AS sql, "
	           "NULL::VARCHAR[] AS labels, NULL::BIGINT AS estimated_size FROM duckdb_schemas() "
	           "WHERE database_name = current_database() AND schema_name <> 'main' AND NOT internal "
	           "UNION ALL "
	           "SELECT 1, schema_name, type_name, NULL::VARCHAR, labels, NULL::BIGINT FROM duckdb_types() "
	           "WHERE database_name = current_database() AND NOT internal AND labels IS NOT NULL "
	           "UNION ALL "
	           "SELECT 2, schema_name, table_name, sql, NULL::VARCHAR[], estimated_size FROM duckdb_tables() "
	           "WHERE database_name = current_database() AND NOT internal "
	           "ORDER BY entry_order, schema_name, entry_name",
	           {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	            LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BIGINT},
	           [&](DataChunk &chunk, idx_t row_idx) {
		           auto entry_order = chunk.GetValue(0, row_idx).GetValue<int32_t>();
		           auto schema_name = chunk.GetValue(1, row_idx).GetValue<string>();
		           if (entry_order == 0) {
			           LoadSchema(schema_name);
			           return;
		           }

		           auto entry_name = chunk.GetValue(2, row_idx).GetValue<string>();
		           auto &schema = catalog.GetSchema(transaction, schema_name).Cast<DuckherderSchemaCatalogEntry>();
		           if (entry_order == 1) {
			           LoadEnumType(schema, entry_name, chunk.GetValue(4, row_idx));
			           return;
		           }
		           LoadTable(schema, entry_name, chunk.GetValue(3, row_idx).GetValue<string>(),
		                     chunk.GetValue(5, row_idx));
	           });
}

void DuckherderCatalogLoader::LoadSchema(const string &schema_name) {
	CreateSchemaInfo info;
	info.schema = schema_name;
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchemaLocal(transaction, info);
}

void DuckherderCatalogLoader::LoadEnumType(DuckherderSchemaCatalogEntry &schema, const string &type_name,
                                           const Value &labels) {
	vector<string> label_literals;
	for (auto &label : ListValue::GetChildren(labels)) {
		label_literals.push_back(label.ToSQLString());
	}
	auto sql = StringUtil::Format("CREATE TYPE %s AS ENUM (%s)", QualifiedRemoteName(schema.name, type_name),
	                              StringUtil::Join(label_literals, ", "));
	auto info = ParseCreateInfo(sql);
	auto &type_info = info->Cast<CreateTypeInfo>();
	type_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	schema.CreateTypeLocal(transaction, type_info);
}

void DuckherderCatalogLoader::LoadTable(DuckherderSchemaCatalogEntry &schema, const string &table_name,
                                        const string &sql, const Value &estimated_size) {
	auto info = unique_ptr_cast<CreateInfo, CreateTableInfo>(ParseCreateInfo(sql));
	info->schema = schema.name;
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);
	schema.CreateTableLocal(transaction, *bound_info);

	if (!estimated_size.IsNull()) {
		catalog.SetEstimatedCardinality(schema.name, table_name, estimated_size.GetValue<idx_t>());
	}
}

} // namespace duckdb
