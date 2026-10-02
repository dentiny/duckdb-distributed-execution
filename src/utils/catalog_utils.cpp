#include "utils/catalog_utils.hpp"

#include "client/execution/distributed_client.hpp"
#include "client/duckherder_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

string GetRemoteStatementSQL(ClientContext &context) {
	const auto &current_query = context.GetCurrentQuery();

	Parser parser;
	parser.ParseQuery(current_query);
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::PREPARE_STATEMENT) {
		auto &prepare = parser.statements[0]->Cast<PrepareStatement>();
		return prepare.statement->ToString();
	}
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::COPY_STATEMENT) {
		auto &copy = parser.statements[0]->Cast<CopyStatement>();
		if (copy.info->is_from && copy.info->file_path_expression) {
			auto binder = Binder::CreateBinder(context);
			binder->Bind(*parser.statements[0]);
			return parser.statements[0]->ToString();
		}
	}
	return current_query;
}

string SanitizeQuery(const string &sql, const string &catalog_name) {
	string result = sql;
	string catalog_prefix = catalog_name + ".";
	size_t pos = 0;
	while ((pos = result.find(catalog_prefix, pos)) != string::npos) {
		result.erase(pos, catalog_prefix.length());
		// Don't increment pos since we just erased characters.
	}
	return result;
}

string GenerateAlterTableSQL(AlterTableInfo &info, const string &table_name) {
	string sql = StringUtil::Format("ALTER TABLE %s ", table_name);

	switch (info.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add_info = info.Cast<AddColumnInfo>();
		sql += StringUtil::Format("ADD COLUMN %s%s %s", add_info.if_column_not_exists ? "IF NOT EXISTS " : "",
		                          add_info.new_column.Name(), add_info.new_column.Type().ToString());
		if (add_info.new_column.HasDefaultValue()) {
			sql += StringUtil::Format(" DEFAULT %s", add_info.new_column.DefaultValue().ToString());
		}
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = info.Cast<RemoveColumnInfo>();
		sql += StringUtil::Format("DROP COLUMN %s%s", remove_info.if_column_exists ? "IF EXISTS " : "",
		                          remove_info.removed_column);
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename_info = info.Cast<RenameColumnInfo>();
		sql += StringUtil::Format("RENAME COLUMN %s TO %s", rename_info.old_name, rename_info.new_name);
		break;
	}
	case AlterTableType::RENAME_TABLE: {
		auto &rename_info = info.Cast<RenameTableInfo>();
		sql = StringUtil::Format("ALTER TABLE %s RENAME TO %s", table_name, rename_info.new_table_name);
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_info = info.Cast<ChangeColumnTypeInfo>();
		sql +=
		    StringUtil::Format("ALTER COLUMN %s TYPE %s", change_info.column_name, change_info.target_type.ToString());
		if (change_info.expression) {
			sql += StringUtil::Format(" USING %s", change_info.expression->ToString());
		}
		break;
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set_default_info = info.Cast<SetDefaultInfo>();
		sql += StringUtil::Format("ALTER COLUMN %s SET DEFAULT %s", set_default_info.column_name,
		                          set_default_info.expression->ToString());
		break;
	}
	case AlterTableType::SET_NOT_NULL: {
		auto &set_not_null_info = info.Cast<SetNotNullInfo>();
		sql += StringUtil::Format("ALTER COLUMN %s SET NOT NULL", set_not_null_info.column_name);
		break;
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop_not_null_info = info.Cast<DropNotNullInfo>();
		sql += StringUtil::Format("ALTER COLUMN %s DROP NOT NULL", drop_not_null_info.column_name);
		break;
	}
	case AlterTableType::ADD_CONSTRAINT: {
		auto &add_constraint_info = info.Cast<AddConstraintInfo>();
		sql += StringUtil::Format("ADD %s", add_constraint_info.constraint->ToString());
		break;
	}
	default:
		throw NotImplementedException("Unsupported ALTER TABLE type for remote execution");
	}

	return sql;
}

DistributedClient &GetDistributedClient(ClientContext &context, TableCatalogEntry &table) {
	auto &catalog = table.ParentCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw InternalException("Expected DuckherderCatalog for distributed operation");
	}
	auto &dh_catalog = catalog.Cast<DuckherderCatalog>();
	return dh_catalog.GetClient(context);
}

DistributedClient &GetDistributedClient(ClientContext &context, Catalog &catalog) {
	if (catalog.GetCatalogType() != "duckherder") {
		throw InternalException("Expected DuckherderCatalog for distributed operation");
	}
	auto &dh_catalog = catalog.Cast<DuckherderCatalog>();
	return dh_catalog.GetClient(context);
}

string QualifiedRemoteName(const string &schema_name, const string &entry_name) {
	return StringUtil::Format("%s.%s", KeywordHelper::WriteQuoted(schema_name, '"'),
	                          KeywordHelper::WriteQuoted(entry_name, '"'));
}

} // namespace duckdb
