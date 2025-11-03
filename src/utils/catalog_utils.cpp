#include "utils/catalog_utils.hpp"

#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

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
	string sql = "ALTER TABLE " + table_name + " ";

	switch (info.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add_info = info.Cast<AddColumnInfo>();
		sql += "ADD COLUMN ";
		if (add_info.if_column_not_exists) {
			sql += "IF NOT EXISTS ";
		}
		sql += add_info.new_column.Name() + " " + add_info.new_column.Type().ToString();
		if (add_info.new_column.HasDefaultValue()) {
			sql += " DEFAULT " + add_info.new_column.DefaultValue().ToString();
		}
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = info.Cast<RemoveColumnInfo>();
		sql += "DROP COLUMN ";
		if (remove_info.if_column_exists) {
			sql += "IF EXISTS ";
		}
		sql += remove_info.removed_column;
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename_info = info.Cast<RenameColumnInfo>();
		sql += "RENAME COLUMN " + rename_info.old_name + " TO " + rename_info.new_name;
		break;
	}
	case AlterTableType::RENAME_TABLE: {
		auto &rename_info = info.Cast<RenameTableInfo>();
		sql = "ALTER TABLE " + table_name + " RENAME TO " + rename_info.new_table_name;
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_info = info.Cast<ChangeColumnTypeInfo>();
		sql += "ALTER COLUMN " + change_info.column_name + " TYPE " + change_info.target_type.ToString();
		break;
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set_default_info = info.Cast<SetDefaultInfo>();
		sql +=
		    "ALTER COLUMN " + set_default_info.column_name + " SET DEFAULT " + set_default_info.expression->ToString();
		break;
	}
	case AlterTableType::SET_NOT_NULL: {
		auto &set_not_null_info = info.Cast<SetNotNullInfo>();
		sql += "ALTER COLUMN " + set_not_null_info.column_name + " SET NOT NULL";
		break;
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop_not_null_info = info.Cast<DropNotNullInfo>();
		sql += "ALTER COLUMN " + drop_not_null_info.column_name + " DROP NOT NULL";
		break;
	}
	default:
		throw NotImplementedException("Unsupported ALTER TABLE type for remote execution");
	}

	return sql;
}

} // namespace duckdb
