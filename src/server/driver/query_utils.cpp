#include "server/driver/query_utils.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

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

std::optional<JoinInputCost> EstimateJoinInputCost(LogicalOperator &input, LogicalGet &scan) {
	if (!input.has_estimated_cardinality && !scan.has_estimated_cardinality) {
		return std::nullopt;
	}
	const auto cardinality = input.has_estimated_cardinality ? input.estimated_cardinality : scan.estimated_cardinality;
	input.ResolveOperatorTypes();
	TupleDataLayout layout;
	layout.Initialize(input.types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	auto row_width = layout.GetRowWidth();
	for (const auto &type : input.types) {
		TypeVisitor::VisitReplace(type, [&](const LogicalType &visited) {
			if (visited.InternalType() == PhysicalType::VARCHAR) {
				row_width += 8;
			} else if (visited.InternalType() == PhysicalType::LIST || visited.InternalType() == PhysicalType::ARRAY) {
				row_width += 32;
			}
			return visited;
		});
	}
	// A build tuple also stores its hash and, on average, three hash-table entries.
	return JoinInputCost {double(cardinality) * double(row_width),
	                      double(cardinality) * double(row_width + sizeof(hash_t) + 3 * sizeof(ht_entry_t))};
}

} // namespace duckdb
