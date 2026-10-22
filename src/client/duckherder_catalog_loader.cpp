#include "client/duckherder_catalog_loader.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_schema_catalog_entry.hpp"
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
	auto result = client.Query(sql, &types);
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

vector<PhysicalIndex> GetKeyIndexes(const ColumnList &columns, const vector<Identifier> &names) {
	vector<PhysicalIndex> indexes;
	indexes.reserve(names.size());
	for (auto &name : names) {
		indexes.emplace_back(columns.GetColumn(name).Physical());
	}
	return indexes;
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
		           auto schema_name = Identifier(chunk.GetValue(1, row_idx).GetValue<string>());
		           if (entry_order == 0) {
			           LoadSchema(schema_name);
			           return;
		           }

		           auto entry_name = Identifier(chunk.GetValue(2, row_idx).GetValue<string>());
		           if (entry_order == 1) {
			           auto &schema = catalog.GetSchema(transaction, schema_name).Cast<DuckherderSchemaCatalogEntry>();
			           LoadEnumType(schema, entry_name, chunk.GetValue(4, row_idx));
			           return;
		           }
		           auto &table = pending_tables[schema_name][entry_name];
		           table.info = unique_ptr_cast<CreateInfo, CreateTableInfo>(
		               ParseCreateInfo(chunk.GetValue(3, row_idx).GetValue<string>()));
		           auto estimated_size = chunk.GetValue(5, row_idx);
		           if (!estimated_size.IsNull()) {
			           table.estimated_size = estimated_size.GetValue<idx_t>();
		           }
	           });

	for (auto &schema_tables : pending_tables) {
		for (auto &table : schema_tables.second) {
			LoadTable(schema_tables.first, table.first, table.second);
		}
	}
}

void DuckherderCatalogLoader::LoadSchema(const Identifier &schema_name) {
	CreateSchemaInfo info;
	info.SetSchema(schema_name);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchemaLocal(transaction, info);
}

void DuckherderCatalogLoader::LoadEnumType(DuckherderSchemaCatalogEntry &schema, const Identifier &type_name,
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

void DuckherderCatalogLoader::LoadTable(const Identifier &schema_name, const Identifier &table_name,
                                        PendingTable &table) {
	if (table.loaded) {
		return;
	}
	table.loaded = true;

	auto &schema = catalog.GetSchema(transaction, schema_name).Cast<DuckherderSchemaCatalogEntry>();
	auto &info = *table.info;
	// The binder looks up referenced tables by catalog name, which is not registered until ATTACH completes, so
	// load referenced tables first and pre-resolve the key indexes. DuckDB rejects foreign keys across schemas.
	for (auto &constraint : info.constraints) {
		if (constraint->type != ConstraintType::FOREIGN_KEY) {
			continue;
		}
		auto &fk = constraint->Cast<ForeignKeyConstraint>();
		if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
			continue;
		}
		fk.info.SetSchema(schema.name);
		fk.info.fk_keys = GetKeyIndexes(info.columns, fk.fk_columns);
		if (fk.info.table == info.GetTableName()) {
			fk.info.type = ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
			fk.info.pk_keys = GetKeyIndexes(info.columns, fk.pk_columns);
			continue;
		}
		auto &schema_tables = pending_tables[schema_name];
		auto pending = schema_tables.find(fk.info.table);
		if (pending == schema_tables.end()) {
			throw CatalogException("Table %s.%s referenced by %s is missing from remote metadata", schema_name,
			                       fk.info.table, table_name);
		}
		LoadTable(schema_name, pending->first, pending->second);
		auto &referenced =
		    schema.GetEntry(transaction, CatalogType::TABLE_ENTRY, fk.info.table)->Cast<TableCatalogEntry>();
		fk.info.pk_keys = GetKeyIndexes(referenced.GetColumns(), fk.pk_columns);
	}

	info.SetSchema(schema.name);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(std::move(table.info), schema);
	schema.CreateTableLocal(transaction, *bound_info);

	if (table.estimated_size.IsValid()) {
		catalog.SetEstimatedCardinality(schema.name, table_name, table.estimated_size.GetIndex());
	}
}

} // namespace duckdb
