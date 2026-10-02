#include "server/driver/query_utils.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

namespace {

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
	default:
		return false;
	}
}

string StripClientCatalog(const string &sql, const string &client_catalog) {
	if (client_catalog.empty()) {
		return sql;
	}

	auto tokens = Parser::Tokenize(sql);
	auto quoted_catalog = KeywordHelper::WriteQuoted(client_catalog, '"');
	string result;
	idx_t cursor = 0;
	for (idx_t index = 0; index + 1 < tokens.size(); index++) {
		auto identifier_end = TokenEnd(sql, tokens, index);
		auto identifier = sql.substr(tokens[index].start, identifier_end - tokens[index].start);
		if (tokens[index].type != SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER ||
		    (!StringUtil::CIEquals(identifier, client_catalog) && identifier != quoted_catalog)) {
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
