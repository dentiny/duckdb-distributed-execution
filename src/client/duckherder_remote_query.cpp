#include "duckherder_remote_query.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_transaction_manager.hpp"

namespace duckdb {

namespace {

struct DuckherderRemoteQueryBindData : public TableFunctionData {
	DuckherderRemoteQueryBindData(string catalog_name_p, string local_table_name_p, string sql_p,
	                              int64_t modification_p)
	    : catalog_name(std::move(catalog_name_p)), local_table_name(std::move(local_table_name_p)),
	      sql(std::move(sql_p)), modification(modification_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckherderRemoteQueryBindData>(catalog_name, local_table_name, sql, modification);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckherderRemoteQueryBindData>();
		return catalog_name == other.catalog_name && local_table_name == other.local_table_name && sql == other.sql &&
		       modification == other.modification;
	}

	string catalog_name;
	string local_table_name;
	string sql;
	int64_t modification;
};

struct DuckherderRemoteQueryState : public GlobalTableFunctionState {
	int64_t rows_affected = 0;
	bool emitted = false;
};

bool IsSupportedInsertSource(InsertStatement &insert) {
	if (insert.GetValuesList()) {
		return true;
	}
	if (!insert.select_statement || insert.select_statement->node->type != QueryNodeType::SELECT_NODE) {
		return false;
	}
	auto &select = insert.select_statement->node->Cast<SelectNode>();
	if (!select.from_table || select.from_table->type != TableReferenceType::TABLE_FUNCTION) {
		return false;
	}
	auto &table_function = select.from_table->Cast<TableFunctionRef>();
	if (!table_function.function || table_function.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &function = table_function.function->Cast<FunctionExpression>();
	return function.function_name == "range" || function.function_name == "generate_series";
}

void ValidateRemoteDML(DuckherderCatalog &catalog, const string &local_table_name, const string &sql,
                       int64_t modification) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw BinderException("Duckherder remote query requires exactly one DML statement");
	}

	auto &statement = *parser.statements[0];
	if (modification == DatabaseModificationType::INSERT_DATA &&
	    statement.type == StatementType::INSERT_STATEMENT) {
		auto &insert = statement.Cast<InsertStatement>();
		BaseTableRef target;
		target.catalog_name = insert.catalog;
		target.schema_name = insert.schema;
		target.table_name = insert.table;
		if (!insert.returning_list.empty() || insert.on_conflict_info || !insert.cte_map.map.empty() ||
		    !IsSupportedInsertSource(insert) || !catalog.IsRegisteredRemoteTarget(local_table_name, target)) {
			throw BinderException("Unsupported or unregistered Duckherder remote INSERT");
		}
		return;
	}
	if (modification != DatabaseModificationType::DELETE_DATA ||
	    statement.type != StatementType::DELETE_STATEMENT) {
		throw BinderException("Duckherder remote query modification does not match DELETE statement");
	}
	auto &del = statement.Cast<DeleteStatement>();
	if (!del.returning_list.empty() || !del.using_clauses.empty() || !del.table ||
	    del.table->type != TableReferenceType::BASE_TABLE || !del.cte_map.map.empty() ||
	    !catalog.IsRegisteredRemoteTarget(local_table_name, del.table->Cast<BaseTableRef>())) {
		throw BinderException("Unsupported or unregistered Duckherder remote DELETE");
	}
}

unique_ptr<FunctionData> BindDuckherderRemoteQuery(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 4) {
		throw BinderException(
		    "duckherder_remote_query requires catalog, local table, SQL, and modification arguments");
	}
	auto modification = input.inputs[3].GetValue<int64_t>();
	if (modification != DatabaseModificationType::INSERT_DATA &&
	    modification != DatabaseModificationType::DELETE_DATA) {
		throw NotImplementedException("Duckherder remote query only supports INSERT and DELETE");
	}
	auto catalog_name = input.inputs[0].GetValue<string>();
	auto local_table_name = input.inputs[1].GetValue<string>();
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	if (catalog.GetCatalogType() != "duckherder") {
		throw BinderException("duckherder_remote_query target is not a Duckherder catalog");
	}
	auto sql = input.inputs[2].GetValue<string>();
	ValidateRemoteDML(catalog.Cast<DuckherderCatalog>(), local_table_name, sql, modification);
	if (!input.binder) {
		throw InternalException("duckherder_remote_query requires a binder");
	}
	input.binder->GetStatementProperties().RegisterDBModify(catalog, context,
	                                                        DatabaseModificationType(modification));
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("Count");
	return make_uniq<DuckherderRemoteQueryBindData>(std::move(catalog_name), std::move(local_table_name),
	                                                std::move(sql), modification);
}

unique_ptr<GlobalTableFunctionState> InitDuckherderRemoteQuery(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DuckherderRemoteQueryBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	if (catalog.GetCatalogType() != "duckherder") {
		throw InternalException("duckherder_remote_query target is not a Duckherder catalog");
	}
	// PREPARE can outlive remote-table registration changes. Revalidate the
	// structural target immediately before opening a session or issuing RPC.
	ValidateRemoteDML(catalog.Cast<DuckherderCatalog>(), bind_data.local_table_name, bind_data.sql,
	                  bind_data.modification);
	auto &transaction_manager = catalog.GetAttached().GetTransactionManager().Cast<DuckherderTransactionManager>();
	auto result =
	    transaction_manager.ExecuteRemote(context, bind_data.sql, DatabaseModificationType(bind_data.modification));
	if (result->HasError()) {
		throw IOException("Remote Duckherder statement failed: %s", result->GetError());
	}

	auto state = make_uniq<DuckherderRemoteQueryState>();
	auto materialized = dynamic_cast<MaterializedQueryResult *>(result.get());
	if (materialized && result->ColumnCount() == 1 && materialized->RowCount() == 1) {
		state->rows_affected = materialized->GetValue(0, 0).GetValue<int64_t>();
	}
	return std::move(state);
}

void ExecuteDuckherderRemoteQuery(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<DuckherderRemoteQueryState>();
	if (state.emitted) {
		return;
	}
	output.SetValue(0, 0, Value::BIGINT(state.rows_affected));
	output.SetCardinality(1);
	state.emitted = true;
}

} // namespace

TableFunction GetDuckherderRemoteQueryFunction() {
	return TableFunction("duckherder_remote_query",
	                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                     ExecuteDuckherderRemoteQuery, BindDuckherderRemoteQuery, InitDuckherderRemoteQuery);
}

} // namespace duckdb
