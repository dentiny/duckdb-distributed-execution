#include "client/remote_index_registry.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

void RemoteIndexRegistry::Add(TableCatalogEntry &table, const CreateIndexInfo &info) {
	IndexInfo index_info;
	index_info.is_unique =
	    info.constraint_type == IndexConstraintType::UNIQUE || info.constraint_type == IndexConstraintType::PRIMARY;
	index_info.is_primary = info.constraint_type == IndexConstraintType::PRIMARY;
	index_info.is_foreign = info.constraint_type == IndexConstraintType::FOREIGN;
	index_info.column_set.insert(info.column_ids.begin(), info.column_ids.end());
	auto add_expression_columns = [&](const auto &expressions) {
		for (auto &expression : expressions) {
			ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
			    *expression, [&](const ColumnRefExpression &column_ref) {
				    auto column_name = column_ref.GetColumnName();
				    auto logical_index = table.GetColumnIndex(column_name);
				    index_info.column_set.insert(table.GetColumns().GetColumn(logical_index).Physical().index);
			    });
		}
	};
	if (index_info.column_set.empty()) {
		add_expression_columns(info.expressions);
	}
	if (index_info.column_set.empty()) {
		add_expression_columns(info.parsed_expressions);
	}

	concurrency::lock_guard<concurrency::mutex> lck(mu);
	indexes[table.name].push_back({info.index_name, std::move(index_info)});
}

vector<IndexInfo> RemoteIndexRegistry::Get(const string &table_name) {
	concurrency::lock_guard<concurrency::mutex> lck(mu);
	auto entry = indexes.find(table_name);
	if (entry == indexes.end()) {
		return {};
	}
	vector<IndexInfo> result;
	result.reserve(entry->second.size());
	for (auto &index : entry->second) {
		result.push_back(index.info);
	}
	return result;
}

void RemoteIndexRegistry::RemoveIndex(const string &index_name) {
	concurrency::lock_guard<concurrency::mutex> lck(mu);
	for (auto &table_indexes : indexes) {
		auto &table_index_list = table_indexes.second;
		table_index_list.erase(
		    std::remove_if(table_index_list.begin(), table_index_list.end(),
		                   [&](const RemoteIndexMetadata &index) { return index.name == index_name; }),
		    table_index_list.end());
	}
}

void RemoteIndexRegistry::RemoveTable(const string &table_name) {
	concurrency::lock_guard<concurrency::mutex> lck(mu);
	indexes.erase(table_name);
}

} // namespace duckdb
