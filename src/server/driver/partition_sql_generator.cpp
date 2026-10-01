#include "server/driver/partition_sql_generator.hpp"

#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

string PartitionSQLGenerator::InjectWhereClause(const string &sql, const string &where_condition) {
	// Locate the WHERE clause in the AST instead of matching keywords in SQL text.
	// AST serialization preserves clause ordering, table aliases, and existing predicate precedence.
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InvalidInputException("Partitioning requires a single SELECT statement");
	}
	auto &statement = parser.statements[0]->Cast<SelectStatement>();
	if (statement.node->type != QueryNodeType::SELECT_NODE) {
		throw InvalidInputException("Partitioning requires a simple SELECT query");
	}
	auto &select = statement.node->Cast<SelectNode>();
	auto predicates = Parser::ParseExpressionList(where_condition);
	if (predicates.size() != 1) {
		throw InvalidInputException("Partitioning requires a single predicate");
	}
	auto predicate = std::move(predicates[0]);
	if (select.where_clause) {
		select.where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
		                                                       std::move(select.where_clause), std::move(predicate));
	} else {
		select.where_clause = std::move(predicate);
	}
	return statement.ToString();
}

} // namespace duckdb
