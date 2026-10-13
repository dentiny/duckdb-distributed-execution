#include "server/driver/query_utils.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

namespace {

idx_t TokenEnd(const string &sql, const vector<SimplifiedToken> &tokens, idx_t index) {
	auto end = index + 1 < tokens.size() ? tokens[index + 1].start : sql.size();
	while (end > tokens[index].start && StringUtil::CharacterIsSpace(sql[end - 1])) {
		end--;
	}
	return end;
}

// The bytes DuckDB's build-side optimizer charges for a tuple of types.
idx_t RowWidth(const vector<LogicalType> &types) {
	TupleDataLayout layout;
	layout.Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	auto width = layout.GetRowWidth();
	for (const auto &type : types) {
		TypeVisitor::VisitReplace(type, [&](const LogicalType &visited) {
			if (visited.InternalType() == PhysicalType::VARCHAR) {
				width += 8;
			} else if (visited.InternalType() == PhysicalType::LIST || visited.InternalType() == PhysicalType::ARRAY) {
				width += 32;
			}
			return visited;
		});
	}
	return width;
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
	auto table = scan.GetTable();
	if (!table || (!input.has_estimated_cardinality && !scan.has_estimated_cardinality)) {
		return std::nullopt;
	}
	// Zone-map pruning is ignored, since it would need row group metadata at planning time.
	vector<LogicalType> read_types;
	for (const auto &column : scan.GetColumnIds()) {
		read_types.emplace_back(scan.GetColumnType(column));
	}
	const auto rows = table->GetStorage().GetTotalRows();
	const auto cardinality = input.has_estimated_cardinality ? input.estimated_cardinality : scan.estimated_cardinality;
	input.ResolveOperatorTypes();
	// A build tuple also stores its hash and, on average, three hash-table entries.
	return JoinInputCost {double(rows) * double(RowWidth(read_types)),
	                      double(cardinality) *
	                          double(RowWidth(input.types) + sizeof(hash_t) + 3 * sizeof(ht_entry_t))};
}

bool ShouldPartitionJoin(const JoinInputCost &partitioned, const vector<JoinInputCost> &replicated, idx_t num_tasks,
                         double max_build_bytes) {
	const auto tasks = double(num_tasks);
	// Tasks share storage but not CPUs, so a replicated read also pays half of each other task's copy.
	const auto contention = (tasks + 1) / 2;
	double local = partitioned.scan_bytes;
	double task = partitioned.scan_bytes / tasks;
	double task_builds = 0;
	for (const auto &input : replicated) {
		local += input.scan_bytes + MinValue(input.build_bytes, partitioned.build_bytes);
		// A task builds on the smaller side: this input or its share of the partitioned one.
		const auto build = MinValue(input.build_bytes, partitioned.build_bytes / tasks);
		task += contention * input.scan_bytes + build;
		task_builds += build;
	}
	return task_builds <= max_build_bytes && task < local;
}

} // namespace duckdb
