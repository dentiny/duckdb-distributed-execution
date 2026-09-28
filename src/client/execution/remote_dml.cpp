#include "client/execution/remote_dml.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "utils/catalog_utils.hpp"

namespace duckdb {

namespace {

struct RemoteDMLSourceState : public GlobalSourceState {
	bool executed = false;
};

StatementType GetDMLStatementType(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::INSERT:
		return StatementType::INSERT_STATEMENT;
	case PhysicalOperatorType::DELETE_OPERATOR:
		return StatementType::DELETE_STATEMENT;
	case PhysicalOperatorType::UPDATE:
		return StatementType::UPDATE_STATEMENT;
	default:
		throw InternalException("Unsupported remote DML operator");
	}
}

string BuildRemotePreparedDMLSQL(ClientContext &context, const string &sql) {
	Parser parser;
	parser.ParseQuery(context.GetCurrentQuery());
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::EXECUTE_STATEMENT) {
		return sql;
	}

	auto &execute = parser.statements[0]->Cast<ExecuteStatement>();
	if (execute.named_values.empty()) {
		return sql;
	}

	// The prepared statement exists only in the client DuckDB connection. Recreate it under a unique name on the
	// client's Control Node connection so DuckDB can bind the EXECUTE arguments without unsafe textual substitution.
	auto statement_name = KeywordHelper::WriteQuoted(
	    StringUtil::Format("__duckherder_remote_%s", UUID::ToString(UUID::GenerateRandomUUID())), '"');
	vector<string> arguments;
	arguments.reserve(execute.named_values.size());
	for (auto &entry : execute.named_values) {
		arguments.push_back(
		    StringUtil::Format("%s := %s", KeywordHelper::WriteQuoted(entry.first, '"'), entry.second->ToString()));
	}
	auto statement_sql = sql;
	StringUtil::RTrim(statement_sql);
	auto prepare_sql = StringUtil::Format("PREPARE %s AS %s", statement_name, statement_sql);
	if (prepare_sql.back() != ';') {
		prepare_sql = StringUtil::Format("%s;", prepare_sql);
	}
	return StringUtil::Format("%s\nEXECUTE %s(%s);\nDEALLOCATE %s;", prepare_sql, statement_name,
	                          StringUtil::Join(arguments, ", "), statement_name);
}

} // namespace

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

	auto executable_sql = BuildRemotePreparedDMLSQL(context.client, sql);
	auto result = GetDistributedClient(context.client, table)
	                  .ExecuteStatement(executable_sql, GetDMLStatementType(type), table.catalog.GetName());
	if (result->HasError()) {
		throw Exception(ExceptionType::IO,
		                StringUtil::Format("Failed to execute DML on control node: %s", result->GetError()));
	}
	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
