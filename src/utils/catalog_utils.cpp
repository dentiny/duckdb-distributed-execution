#include "utils/catalog_utils.hpp"

#include "client/execution/distributed_client.hpp"
#include "client/duckherder_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

string GetRemoteStatementSQL(ClientContext &context) {
	const auto &current_query = context.GetCurrentQuery();

	auto parser = Parser::GetBuiltinParser();
	parser.ParseQuery(current_query);
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::PREPARE_STATEMENT) {
		auto &prepare = parser.statements[0]->Cast<PrepareStatement>();
		return prepare.statement->ToString();
	}
	// DuckDB may plan a prepared statement again when it is executed.
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::EXECUTE_STATEMENT) {
		auto &execute = parser.statements[0]->Cast<ExecuteStatement>();
		auto &prepared_statements = ClientData::Get(context).prepared_statements;
		auto entry = prepared_statements.find(execute.name);
		if (entry != prepared_statements.end() && entry->second->unbound_statement) {
			return entry->second->unbound_statement->ToString();
		}
	}
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::COPY_STATEMENT) {
		auto &copy = parser.statements[0]->Cast<CopyStatement>();
		if (copy.info->is_from && copy.info->file_path_expression) {
			auto binder = Binder::CreateBinder(context);
			binder->Bind(*parser.statements[0]);
			return parser.statements[0]->ToString();
		}
	}
	return current_query;
}

DistributedClient &GetDistributedClient(ClientContext &context, TableCatalogEntry &table) {
	return GetDistributedClient(context, table.ParentCatalog());
}

DistributedClient &GetDistributedClient(ClientContext &context, Catalog &catalog) {
	if (catalog.GetCatalogType() != "duckherder") {
		throw InternalException("Expected DuckherderCatalog for distributed operation");
	}
	auto &dh_catalog = catalog.Cast<DuckherderCatalog>();
	return dh_catalog.GetClient(context);
}

string QualifiedRemoteName(const Identifier &schema_name, const Identifier &entry_name) {
	return StringUtil::Format("%s.%s", SQLQuotedIdentifier::ToString(schema_name),
	                          SQLQuotedIdentifier::ToString(entry_name));
}

} // namespace duckdb
