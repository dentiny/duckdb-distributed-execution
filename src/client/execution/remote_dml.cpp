#include "client/execution/remote_dml.hpp"

#include "client/duckherder_catalog.hpp"
#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

struct RemoteDMLSourceState : public GlobalSourceState {
	bool executed = false;
};

void RewriteQueryNode(QueryNode &node, DuckherderCatalog &catalog);

void RewriteCTEs(CommonTableExpressionMap &cte_map, DuckherderCatalog &catalog) {
	for (auto &entry : cte_map.map) {
		if (entry.second && entry.second->query && entry.second->query->node) {
			RewriteQueryNode(*entry.second->query->node, catalog);
		}
	}
}

void RewriteQualifiedTableName(string &catalog_name, string &schema_name, string &table_name,
                               DuckherderCatalog &catalog) {
	if (!catalog.IsRemoteTable(table_name)) {
		return;
	}
	const auto client_catalog = catalog.GetName();
	if (catalog_name == client_catalog) {
		catalog_name = INVALID_CATALOG;
	} else if (catalog_name == INVALID_CATALOG && schema_name == client_catalog) {
		schema_name = INVALID_SCHEMA;
	}
	table_name = catalog.GetRemoteTableConfig(table_name).remote_table_name;
}

void RewriteTableRef(TableRef &ref, DuckherderCatalog &catalog) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		RewriteQualifiedTableName(base.catalog_name, base.schema_name, base.table_name, catalog);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		RewriteTableRef(*join.left, catalog);
		RewriteTableRef(*join.right, catalog);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		RewriteQueryNode(*subquery.subquery->node, catalog);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &table_function = ref.Cast<TableFunctionRef>();
		if (table_function.subquery && table_function.subquery->node) {
			RewriteQueryNode(*table_function.subquery->node, catalog);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref.Cast<PivotRef>();
		RewriteTableRef(*pivot.source, catalog);
		for (auto &column : pivot.pivots) {
			if (column.subquery) {
				RewriteQueryNode(*column.subquery, catalog);
			}
		}
		break;
	}
	default:
		break;
	}
}

void RewriteQueryNode(QueryNode &node, DuckherderCatalog &catalog) {
	RewriteCTEs(node.cte_map, catalog);
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			RewriteTableRef(*select.from_table, catalog);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &set_operation = node.Cast<SetOperationNode>();
		for (auto &child : set_operation.children) {
			RewriteQueryNode(*child, catalog);
		}
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &recursive_cte = node.Cast<RecursiveCTENode>();
		RewriteQueryNode(*recursive_cte.left, catalog);
		RewriteQueryNode(*recursive_cte.right, catalog);
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte = node.Cast<CTENode>();
		RewriteQueryNode(*cte.query, catalog);
		RewriteQueryNode(*cte.child, catalog);
		break;
	}
	default:
		break;
	}
}

} // namespace

string RewriteRemoteDMLStatement(ClientContext &context, DuckherderCatalog &catalog) {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(context.GetCurrentQuery());
	if (parser.statements.size() != 1) {
		throw InvalidInputException("Remote DML requires exactly one SQL statement");
	}

	auto &statement = *parser.statements[0];
	switch (statement.type) {
	case StatementType::INSERT_STATEMENT: {
		auto &insert = statement.Cast<InsertStatement>();
		if (!insert.returning_list.empty()) {
			throw NotImplementedException("RETURNING is not supported for remote DML");
		}
		RewriteQualifiedTableName(insert.catalog, insert.schema, insert.table, catalog);
		RewriteCTEs(insert.cte_map, catalog);
		if (insert.select_statement && insert.select_statement->node) {
			RewriteQueryNode(*insert.select_statement->node, catalog);
		}
		break;
	}
	case StatementType::DELETE_STATEMENT: {
		auto &delete_statement = statement.Cast<DeleteStatement>();
		if (!delete_statement.returning_list.empty()) {
			throw NotImplementedException("RETURNING is not supported for remote DML");
		}
		RewriteCTEs(delete_statement.cte_map, catalog);
		RewriteTableRef(*delete_statement.table, catalog);
		for (auto &using_clause : delete_statement.using_clauses) {
			RewriteTableRef(*using_clause, catalog);
		}
		break;
	}
	case StatementType::UPDATE_STATEMENT: {
		auto &update = statement.Cast<UpdateStatement>();
		if (!update.returning_list.empty()) {
			throw NotImplementedException("RETURNING is not supported for remote DML");
		}
		RewriteCTEs(update.cte_map, catalog);
		RewriteTableRef(*update.table, catalog);
		if (update.from_table) {
			RewriteTableRef(*update.from_table, catalog);
		}
		break;
	}
	default:
		throw InvalidInputException("Remote DML only supports INSERT, DELETE, and UPDATE statements");
	}
	return statement.ToString();
}

PhysicalRemoteDML::PhysicalRemoteDML(PhysicalPlan &physical_plan, PhysicalOperatorType operator_type,
                                     vector<LogicalType> types, TableCatalogEntry &table_p, string sql_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, operator_type, std::move(types), estimated_cardinality), table(table_p),
      sql(std::move(sql_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoteDML::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoteDMLSourceState>();
}

SourceResultType PhysicalRemoteDML::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<RemoteDMLSourceState>();
	if (state.executed) {
		return SourceResultType::FINISHED;
	}
	state.executed = true;

	auto result = GetDistributedClient(table).ExecuteSQL(sql);
	if (result->HasError()) {
		throw Exception(ExceptionType::IO, "Failed to execute DML on control node: " + result->GetError());
	}
	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
