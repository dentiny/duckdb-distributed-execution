#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "server/driver/query_utils.hpp"

namespace duckdb {

namespace {

// Column qualifiers belong to their query scope, not necessarily to a catalog. Keep their source locations so
// catalog removal can preserve the original SQL, including ALTER identifiers and string literals.
class ClientCatalogColumnReferences {
public:
	explicit ClientCatalogColumnReferences(const string &catalog_p) : catalog(catalog_p) {
	}

	void Visit(SQLStatement &statement) {
		const auto outer_tables = tables.size();
		switch (statement.type) {
		case StatementType::SELECT_STATEMENT:
			VisitQuery(*statement.Cast<SelectStatement>().node, false);
			break;
		case StatementType::DELETE_STATEMENT: {
			auto &remove = statement.Cast<DeleteStatement>();
			AddTables(remove.table.get());
			bool alias = HasAlias(remove.table.get());
			for (auto &ref : remove.using_clauses) {
				AddTables(ref.get());
				alias = alias || HasAlias(ref.get());
			}
			VisitTable(remove.table.get(), alias);
			for (auto &ref : remove.using_clauses) {
				VisitTable(ref.get(), alias);
			}
			VisitExpression(remove.condition.get(), alias);
			VisitExpressions(remove.returning_list, alias);
			VisitCTEs(remove.cte_map, false);
			break;
		}
		case StatementType::UPDATE_STATEMENT: {
			auto &update = statement.Cast<UpdateStatement>();
			AddTables(update.table.get());
			AddTables(update.from_table.get());
			const bool alias = HasAlias(update.table.get()) || HasAlias(update.from_table.get());
			VisitTable(update.table.get(), alias);
			VisitTable(update.from_table.get(), alias);
			VisitUpdate(*update.set_info, alias);
			VisitExpressions(update.returning_list, alias);
			VisitCTEs(update.cte_map, false);
			break;
		}
		case StatementType::INSERT_STATEMENT: {
			auto &insert = statement.Cast<InsertStatement>();
			if (insert.select_statement) {
				Visit(*insert.select_statement);
			}
			AddTables(insert.table_ref.get());
			const bool alias = HasAlias(insert.table_ref.get()) || StringUtil::CIEquals(insert.table, catalog);
			VisitExpressions(insert.returning_list, alias);
			if (insert.on_conflict_info) {
				auto &conflict = *insert.on_conflict_info;
				VisitExpression(conflict.condition.get(), alias);
				if (conflict.set_info) {
					VisitUpdate(*conflict.set_info, alias);
				}
			}
			VisitCTEs(insert.cte_map, false);
			break;
		}
		case StatementType::MERGE_INTO_STATEMENT: {
			auto &merge = statement.Cast<MergeIntoStatement>();
			AddTables(merge.target.get());
			AddTables(merge.source.get());
			const bool alias = HasAlias(merge.target.get()) || HasAlias(merge.source.get());
			VisitTable(merge.target.get(), alias);
			VisitTable(merge.source.get(), alias);
			VisitExpression(merge.join_condition.get(), alias);
			for (auto &entry : merge.actions) {
				for (auto &action : entry.second) {
					VisitExpression(action->condition.get(), alias);
					VisitExpressions(action->expressions, alias);
					if (action->update_info) {
						VisitUpdate(*action->update_info, alias);
					}
				}
			}
			VisitExpressions(merge.returning_list, alias);
			VisitCTEs(merge.cte_map, false);
			break;
		}
		case StatementType::PREPARE_STATEMENT:
			Visit(*statement.Cast<PrepareStatement>().statement);
			break;
		case StatementType::CREATE_STATEMENT: {
			auto &info = *statement.Cast<CreateStatement>().info;
			if (info.type == CatalogType::TABLE_ENTRY && info.Cast<CreateTableInfo>().query) {
				Visit(*info.Cast<CreateTableInfo>().query);
			} else if (info.type == CatalogType::VIEW_ENTRY) {
				Visit(*info.Cast<CreateViewInfo>().query);
			}
			break;
		}
		case StatementType::COPY_STATEMENT: {
			auto &info = *statement.Cast<CopyStatement>().info;
			if (info.select_statement) {
				VisitQuery(*info.select_statement, false);
			}
			break;
		}
		default:
			break;
		}
		tables.resize(outer_tables);
	}

	unordered_set<idx_t> locations;

private:
	void AddTables(optional_ptr<TableRef> ref) {
		if (!ref) {
			return;
		}
		if (ref->type == TableReferenceType::BASE_TABLE) {
			tables.push_back(&ref->Cast<BaseTableRef>());
		} else if (ref->type == TableReferenceType::JOIN && ref->alias.empty()) {
			auto &join = ref->Cast<JoinRef>();
			AddTables(join.left.get());
			AddTables(join.right.get());
		}
	}

	bool IsCatalogColumn(const vector<string> &names) const {
		if (names.size() < 3) {
			return false;
		}
		for (auto *table : tables) {
			if (!table->catalog_name.empty() && !StringUtil::CIEquals(table->catalog_name, catalog)) {
				continue;
			}
			const auto &name = table->alias.empty() ? table->table_name : table->alias;
			const auto schema = table->schema_name.empty() || StringUtil::CIEquals(table->schema_name, catalog)
			                        ? DEFAULT_SCHEMA
			                        : table->schema_name.c_str();
			// Like DuckDB's binder, try catalog.table.column and catalog.schema.table.column before alias fields.
			if (StringUtil::CIEquals(names[1], name) ||
			    (names.size() >= 4 && StringUtil::CIEquals(names[1], schema) && StringUtil::CIEquals(names[2], name))) {
				return true;
			}
		}
		return false;
	}

	bool HasAlias(optional_ptr<TableRef> ref) const {
		if (!ref) {
			return false;
		}
		if (!ref->alias.empty()) {
			return StringUtil::CIEquals(ref->alias, catalog);
		}
		if (ref->type == TableReferenceType::BASE_TABLE) {
			return StringUtil::CIEquals(ref->Cast<BaseTableRef>().table_name, catalog);
		}
		if (ref->type == TableReferenceType::JOIN) {
			auto &join = ref->Cast<JoinRef>();
			return HasAlias(join.left.get()) || HasAlias(join.right.get());
		}
		return false;
	}

	void VisitExpression(optional_ptr<ParsedExpression> expr, bool alias) {
		if (!expr) {
			return;
		}
		if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &column = expr->Cast<ColumnRefExpression>();
			// Two-part columns are table/struct references. Longer references may also start with a visible alias.
			if (column.column_names.size() >= 2 &&
			    (column.column_names.size() == 2 || (alias && !IsCatalogColumn(column.column_names))) &&
			    StringUtil::CIEquals(column.column_names[0], catalog) && column.GetQueryLocation().IsValid()) {
				locations.insert(column.GetQueryLocation().GetIndex());
			}
		} else if (expr->GetExpressionClass() == ExpressionClass::STAR) {
			if (StringUtil::CIEquals(expr->Cast<StarExpression>().relation_name, catalog) &&
			    expr->GetQueryLocation().IsValid()) {
				locations.insert(expr->GetQueryLocation().GetIndex());
			}
		} else if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
			VisitQuery(*expr->Cast<SubqueryExpression>().subquery->node, alias);
		}
		ParsedExpressionIterator::EnumerateChildren(*expr,
		                                            [&](ParsedExpression &child) { VisitExpression(child, alias); });
	}

	void VisitExpressions(vector<unique_ptr<ParsedExpression>> &expressions, bool alias) {
		for (auto &expr : expressions) {
			VisitExpression(expr.get(), alias);
		}
	}

	void VisitUpdate(UpdateSetInfo &update, bool alias) {
		VisitExpression(update.condition.get(), alias);
		VisitExpressions(update.expressions, alias);
	}

	void VisitCTEs(CommonTableExpressionMap &ctes, bool alias) {
		for (auto &entry : ctes.map) {
			VisitQuery(*entry.second->query->node, alias);
		}
	}

	void VisitTable(optional_ptr<TableRef> ref, bool alias) {
		if (!ref) {
			return;
		}
		if (ref->type == TableReferenceType::SUBQUERY) {
			VisitQuery(*ref->Cast<SubqueryRef>().subquery->node, alias);
		} else if (ref->type == TableReferenceType::JOIN) {
			auto &join = ref->Cast<JoinRef>();
			VisitTable(join.left.get(), alias);
			VisitTable(join.right.get(), alias);
			VisitExpression(join.condition.get(), alias);
		} else {
			ParsedExpressionIterator::EnumerateTableRefChildren(
			    *ref, [&](unique_ptr<ParsedExpression> &expr) { VisitExpression(expr.get(), alias); });
		}
	}

	void VisitQuery(QueryNode &node, bool outer_alias) {
		const auto outer_tables = tables.size();
		if (node.type == QueryNodeType::SELECT_NODE) {
			auto &select = node.Cast<SelectNode>();
			AddTables(select.from_table.get());
			const bool alias = outer_alias || HasAlias(select.from_table.get());
			VisitTable(select.from_table.get(), alias);
			VisitExpressions(select.select_list, alias);
			VisitExpressions(select.groups.group_expressions, alias);
			VisitExpression(select.where_clause.get(), alias);
			VisitExpression(select.having.get(), alias);
			VisitExpression(select.qualify.get(), alias);
			ParsedExpressionIterator::EnumerateQueryNodeModifiers(
			    node, [&](unique_ptr<ParsedExpression> &expr) { VisitExpression(expr.get(), alias); });
		} else if (node.type == QueryNodeType::SET_OPERATION_NODE) {
			for (auto &child : node.Cast<SetOperationNode>().children) {
				VisitQuery(*child, outer_alias);
			}
		} else if (node.type == QueryNodeType::RECURSIVE_CTE_NODE) {
			auto &cte = node.Cast<RecursiveCTENode>();
			VisitQuery(*cte.left, outer_alias);
			VisitQuery(*cte.right, outer_alias);
			VisitExpressions(cte.key_targets, outer_alias);
		}
		tables.resize(outer_tables);
		VisitCTEs(node.cte_map, outer_alias);
	}

	const string &catalog;
	vector<BaseTableRef *> tables;
};

idx_t TokenEnd(const string &sql, const vector<SimplifiedToken> &tokens, idx_t index) {
	auto end = index + 1 < tokens.size() ? tokens[index + 1].start : sql.size();
	while (end > tokens[index].start && StringUtil::CharacterIsSpace(sql[end - 1])) {
		end--;
	}
	return end;
}

} // namespace

bool ContainsTableScan(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsTableScan(child.get())) {
			return true;
		}
	}
	return false;
}

bool IsSupportedPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		if (op.children.size() != 1) {
			return false;
		}
		return IsSupportedPlan(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_GET:
		return true;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		if (op.Cast<LogicalComparisonJoin>().join_type != JoinType::INNER || op.children.size() != 2) {
			return false;
		}
		return IsSupportedPlan(*op.children[0]) && IsSupportedPlan(*op.children[1]);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return op.children.size() == 2 && IsSupportedPlan(*op.children[0]) && IsSupportedPlan(*op.children[1]);
	default:
		return false;
	}
}

bool IsSimplePartitionedJoin(const SelectStatement &statement) {
	if (!statement.named_param_map.empty() || statement.node->type != QueryNodeType::SELECT_NODE) {
		return false;
	}
	const auto &select = statement.node->Cast<SelectNode>();
	if (!select.from_table || select.from_table->type != TableReferenceType::JOIN || !select.cte_map.map.empty() ||
	    !select.modifiers.empty() || select.sample) {
		return false;
	}
	const auto &join = select.from_table->Cast<JoinRef>();
	return join.type == JoinType::INNER && join.left->type == TableReferenceType::BASE_TABLE &&
	       join.right->type == TableReferenceType::BASE_TABLE;
}

string StripClientCatalog(const string &sql, const string &client_catalog) {
	if (client_catalog.empty()) {
		return sql;
	}

	Parser parser;
	try {
		parser.ParseQuery(sql);
	} catch (const ParserException &) {
		// Let Connection::Query report syntax errors as replayable statement errors, not transport failures.
		return sql;
	}
	ClientCatalogColumnReferences columns(client_catalog);
	for (auto &statement : parser.statements) {
		columns.Visit(*statement);
	}
	auto tokens = Parser::Tokenize(sql);
	auto quoted_catalog = KeywordHelper::WriteQuoted(client_catalog, '"');
	string result;
	idx_t cursor = 0;
	for (idx_t index = 0; index + 1 < tokens.size(); index++) {
		auto identifier_end = TokenEnd(sql, tokens, index);
		auto identifier = sql.substr(tokens[index].start, identifier_end - tokens[index].start);
		if (columns.locations.count(tokens[index].start) ||
		    tokens[index].type != SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER ||
		    (!StringUtil::CIEquals(identifier, client_catalog) && !StringUtil::CIEquals(identifier, quoted_catalog))) {
			continue;
		}

		// A catalog can only start a qualified name; a later component may be a struct field called like the catalog.
		if (index > 0 &&
		    sql.substr(tokens[index - 1].start, TokenEnd(sql, tokens, index - 1) - tokens[index - 1].start) == ".") {
			continue;
		}

		auto dot_end = TokenEnd(sql, tokens, index + 1);
		auto next_token = sql.substr(tokens[index + 1].start, dot_end - tokens[index + 1].start);
		if (tokens[index + 1].type != SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR || next_token != ".") {
			continue;
		}

		result.append(sql, cursor, tokens[index].start - cursor);
		cursor = dot_end;
		index++;
	}
	result.append(sql, cursor, sql.size() - cursor);
	return result;
}

} // namespace duckdb
