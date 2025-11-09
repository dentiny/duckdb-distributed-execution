#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/distributed_executor.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string PartitionSQLGenerator::CreatePartitionSQL(const string &sql, idx_t partition_id, idx_t total_partitions,
                                                 const PlanPartitionInfo &partition_info) {
	string trimmed = sql;
	StringUtil::RTrim(trimmed);
	bool has_semicolon = !trimmed.empty() && trimmed.back() == ';';
	if (has_semicolon) {
		trimmed.pop_back();
		StringUtil::RTrim(trimmed);
	}

	string clause;

	// Use intelligent partitioning based on plan analysis
	if (partition_info.supports_intelligent_partitioning) {
		// Range-based partitioning: more cache-friendly and aligned with row groups
		// Each worker gets a contiguous range of rowids
		idx_t row_start = partition_id * partition_info.rows_per_partition;
		idx_t row_end = (partition_id + 1) * partition_info.rows_per_partition - 1;

		// For the last partition, extend to include any remainder rows
		if (partition_id == total_partitions - 1) {
			row_end = partition_info.estimated_cardinality;
		}

		clause = StringUtil::Format("rowid BETWEEN %llu AND %llu", static_cast<long long unsigned>(row_start),
		                            static_cast<long long unsigned>(row_end));
	} else {
		// Fallback: Modulo-based partitioning
		// This is used for small tables, non-table-scan operators, or when cardinality is unknown
		clause = StringUtil::Format("(rowid %% %llu) = %llu", static_cast<long long unsigned>(total_partitions),
		                            static_cast<long long unsigned>(partition_id));
	}

	string partition_sql = trimmed + " WHERE " + clause;
	if (has_semicolon) {
		partition_sql += ";";
	}
	return partition_sql;
}

string PartitionSQLGenerator::InjectWhereClause(const string &sql, const string &where_condition) {
	string trimmed = sql;
	StringUtil::RTrim(trimmed);
	if (!trimmed.empty() && trimmed.back() == ';') {
		trimmed.pop_back();
		StringUtil::RTrim(trimmed);
	}

	// Convert to uppercase for keyword matching
	string upper_sql = StringUtil::Upper(trimmed);

	// Find the position to insert WHERE clause
	// It should go after FROM/JOIN but before WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET
	vector<string> after_keywords = {"FROM", "JOIN"};
	vector<string> before_keywords = {"WHERE",  "GROUP BY", "HAVING",    "ORDER BY", "LIMIT",
	                                  "OFFSET", "UNION",    "INTERSECT", "EXCEPT"};

	// Find the last occurrence of FROM or JOIN
	idx_t insert_pos = string::npos;
	for (const auto &keyword : after_keywords) {
		idx_t pos = upper_sql.rfind(keyword);
		if (pos != string::npos) {
			// Find the end of the table name/clause after this keyword
			// Simple heuristic: find the next space after the table identifier
			idx_t start = pos + keyword.length();

			// Skip the table name and any aliases
			// Look for the next keyword or end of string
			idx_t next_keyword_pos = trimmed.length();
			for (const auto &next_kw : before_keywords) {
				idx_t kw_pos = upper_sql.find(next_kw, start);
				if (kw_pos != string::npos && kw_pos < next_keyword_pos) {
					next_keyword_pos = kw_pos;
				}
			}

			insert_pos = next_keyword_pos;
			break;
		}
	}

	// If no FROM found, check if there's already a WHERE clause
	if (insert_pos == string::npos) {
		// Check if WHERE already exists
		idx_t where_pos = upper_sql.find("WHERE");
		if (where_pos != string::npos) {
			// Already has WHERE, append to it with AND
			return trimmed + " AND " + where_condition;
		} else {
			// No FROM and no WHERE - just append
			return trimmed + " WHERE " + where_condition;
		}
	}

	// Check if there's already a WHERE clause before our insert position
	idx_t existing_where = upper_sql.find("WHERE");
	if (existing_where != string::npos && existing_where < insert_pos) {
		// WHERE already exists, append with AND
		// Find the end of the WHERE clause (before GROUP BY, HAVING, etc.)
		idx_t where_end = insert_pos;
		for (const auto &keyword : before_keywords) {
			if (keyword == "WHERE")
				continue;
			idx_t kw_pos = upper_sql.find(keyword, existing_where);
			if (kw_pos != string::npos && kw_pos < where_end) {
				where_end = kw_pos;
			}
		}

		string before = trimmed.substr(0, where_end);
		string after = trimmed.substr(where_end);
		StringUtil::RTrim(before);
		StringUtil::LTrim(after);
		return before + " AND " + where_condition + (after.empty() ? "" : " " + after);
	}

	// Insert WHERE clause at the correct position
	string before = trimmed.substr(0, insert_pos);
	string after = trimmed.substr(insert_pos);
	StringUtil::RTrim(before);
	StringUtil::LTrim(after);

	return before + " WHERE " + where_condition + (after.empty() ? "" : " " + after);
}

} // namespace duckdb
