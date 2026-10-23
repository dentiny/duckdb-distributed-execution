#include "client/execution/distributed_query_pushdown.hpp"

#include "arrow_record_batch_converter.hpp"
#include "client/execution/distributed_client.hpp"
#include "duckherder_catalog.hpp"
#include "duckherder_schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_transaction.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "utils/catalog_utils.hpp"
#include "utils/sql_render_utils.hpp"

namespace duckdb {

namespace {

constexpr const char *REMOTE_QUERY_FUNCTION = "duckherder_query";
constexpr const char *CONNECTED_QUERY_FUNCTION = "duckherder_connected_query";

bool DependsOnTimeZone(LogicalOperator &op);

// Passing a TIMESTAMPTZ value through does not depend on the time zone, but computing on one may.
bool DependsOnTimeZone(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_REF:
		return false;
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &plan = expr.Cast<BoundSubqueryExpression>().Subquery().plan;
		if (plan && DependsOnTimeZone(*plan)) {
			return true;
		}
		break;
	}
	default:
		break;
	}
	if (DependsOnTimeZone(expr.GetReturnType())) {
		return true;
	}
	bool depends = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		depends = depends || DependsOnTimeZone(child.GetReturnType()) || DependsOnTimeZone(child);
	});
	return depends;
}

bool DependsOnTimeZone(LogicalOperator &op) {
	bool depends = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expr) { depends = depends || DependsOnTimeZone(**expr); });
	for (auto &child : op.children) {
		depends = depends || DependsOnTimeZone(*child);
	}
	return depends;
}

// The server runs queries with default settings, so the client must not have changed any affecting query results.
bool HasDefaultQuerySettings(ClientContext &context) {
	if (Settings::Get<DefaultOrderSetting>(context) !=
	    EnumUtil::FromString<OrderType>(DefaultOrderSetting::DefaultValue)) {
		return false;
	}
	if (Settings::Get<DefaultNullOrderSetting>(context) !=
	    EnumUtil::FromString<DefaultOrderByNullType>(DefaultNullOrderSetting::DefaultValue)) {
		return false;
	}
	if (Settings::Get<DefaultCollationSetting>(context) != DefaultCollationSetting::DefaultValue) {
		return false;
	}
	if (Settings::Get<IntegerDivisionSetting>(context) !=
	    StringUtil::Equals(IntegerDivisionSetting::DefaultValue, "true")) {
		return false;
	}
	if (Settings::Get<IeeeFloatingPointOpsSetting>(context) !=
	    StringUtil::Equals(IeeeFloatingPointOpsSetting::DefaultValue, "true")) {
		return false;
	}
	if (Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(context) !=
	    StringUtil::Equals(ScalarSubqueryErrorOnMultipleRowsSetting::DefaultValue, "true")) {
		return false;
	}
	if (Settings::Get<OldImplicitCastingSetting>(context) !=
	    StringUtil::Equals(OldImplicitCastingSetting::DefaultValue, "true")) {
		return false;
	}
	if (Settings::Get<OrderByNonIntegerLiteralSetting>(context) !=
	    StringUtil::Equals(OrderByNonIntegerLiteralSetting::DefaultValue, "true")) {
		return false;
	}
	return true;
}

bool QueryPushdownEnabled(ClientContext &context) {
	Value enabled;
	context.TryGetCurrentSetting(Identifier(QUERY_PUSHDOWN_SETTING), enabled);
	return enabled.GetValue<bool>();
}

// Functions reading the session state, environment or sequences, which differ on the server. Functions that DuckDB
// evaluates once per query, such as current_date, are folded into constants before pushdown instead.
bool ReadsSessionState(const Identifier &function_name) {
	static const identifier_set_t SESSION_FUNCTIONS {"currval",
	                                                 "current_catalog",
	                                                 "current_connection_id",
	                                                 "current_database",
	                                                 "current_date",
	                                                 "current_localtime",
	                                                 "current_localtimestamp",
	                                                 "current_query",
	                                                 "current_role",
	                                                 "current_schema",
	                                                 "current_schemas",
	                                                 "current_setting",
	                                                 "current_transaction_id",
	                                                 "current_user",
	                                                 "getenv",
	                                                 "getvariable",
	                                                 "in_search_path",
	                                                 "nextval",
	                                                 "session_user",
	                                                 "setseed",
	                                                 "today",
	                                                 "txid_current",
	                                                 "user"};
	return SESSION_FUNCTIONS.find(function_name) != SESSION_FUNCTIONS.end();
}

// Client-side types are unknown to the server.
bool IsBuiltinType(const TypeExpression &type) {
	return type.GetCatalog().empty() && type.GetSchema().empty() &&
	       DefaultTypeGenerator::GetDefaultType(type.GetTypeName()) != LogicalTypeId::INVALID;
}

// Visits the base tables and functions of a pushed-down query, including those in CTEs and subqueries. A table is
// passed with whether it names a CTE in scope where it is referenced.
class PushedQueryVisitor {
public:
	PushedQueryVisitor(std::function<void(BaseTableRef &, bool is_cte)> table_callback_p,
	                   std::function<void(FunctionExpression &)> function_callback_p)
	    : table_callback(std::move(table_callback_p)), function_callback(std::move(function_callback_p)) {
	}

	void VisitNode(QueryNode &node) {
		idx_t scope_size = cte_scope.size();
		for (auto &cte : node.cte_map.map) {
			cte_scope.push_back(cte.first);
		}
		for (auto &cte : node.cte_map.map) {
			if (cte.second->query_node) {
				VisitNode(*cte.second->query_node);
			}
		}
		VisitNodeBody(node);
		cte_scope.resize(scope_size);
	}

private:
	void VisitNodeBody(QueryNode &node) {
		switch (node.type) {
		case QueryNodeType::SELECT_NODE: {
			auto &select = node.Cast<SelectNode>();
			for (auto &expr : select.select_list) {
				VisitExpression(*expr);
			}
			if (select.from_table) {
				VisitTableRef(*select.from_table);
			}
			if (select.where_clause) {
				VisitExpression(*select.where_clause);
			}
			for (auto &expr : select.groups.group_expressions) {
				VisitExpression(*expr);
			}
			if (select.having) {
				VisitExpression(*select.having);
			}
			if (select.qualify) {
				VisitExpression(*select.qualify);
			}
			break;
		}
		case QueryNodeType::SET_OPERATION_NODE:
			for (auto &child : node.Cast<SetOperationNode>().children) {
				VisitNode(*child);
			}
			break;
		case QueryNodeType::RECURSIVE_CTE_NODE: {
			auto &cte = node.Cast<RecursiveCTENode>();
			cte_scope.push_back(cte.ctename);
			VisitNode(*cte.left);
			VisitNode(*cte.right);
			for (auto &expr : cte.key_targets) {
				VisitExpression(*expr);
			}
			cte_scope.pop_back();
			break;
		}
		case QueryNodeType::INSERT_QUERY_NODE: {
			auto &insert = node.Cast<InsertQueryNode>();
			BaseTableRef target;
			target.SetQualifiedName(insert.qualified_name);
			table_callback(target, false);
			insert.qualified_name = target.GetQualifiedName();
			// The binder turns an INSERT ... ON CONFLICT into a MERGE into this reference to the target.
			if (insert.table_ref && insert.table_ref->type == TableReferenceType::BASE_TABLE) {
				insert.table_ref->Cast<BaseTableRef>().SetQualifiedName(insert.qualified_name);
			}
			if (insert.select_statement) {
				VisitNode(*insert.select_statement->node);
			}
			if (insert.on_conflict_info) {
				if (insert.on_conflict_info->condition) {
					VisitExpression(*insert.on_conflict_info->condition);
				}
				if (insert.on_conflict_info->set_info) {
					VisitUpdateSet(*insert.on_conflict_info->set_info);
				}
			}
			VisitExpressions(insert.returning_list);
			break;
		}
		case QueryNodeType::UPDATE_QUERY_NODE: {
			auto &update = node.Cast<UpdateQueryNode>();
			VisitTableRef(*update.table);
			if (update.from_table) {
				VisitTableRef(*update.from_table);
			}
			if (update.set_info) {
				VisitUpdateSet(*update.set_info);
			}
			VisitExpressions(update.returning_list);
			break;
		}
		case QueryNodeType::DELETE_QUERY_NODE: {
			auto &del = node.Cast<DeleteQueryNode>();
			VisitTableRef(*del.table);
			for (auto &using_clause : del.using_clauses) {
				VisitTableRef(*using_clause);
			}
			if (del.condition) {
				VisitExpression(*del.condition);
			}
			VisitExpressions(del.returning_list);
			break;
		}
		case QueryNodeType::MERGE_QUERY_NODE: {
			auto &merge = node.Cast<MergeQueryNode>();
			VisitTableRef(*merge.target);
			VisitTableRef(*merge.source);
			if (merge.join_condition) {
				VisitExpression(*merge.join_condition);
			}
			for (auto &entry : merge.actions) {
				for (auto &action : entry.second) {
					if (action->condition) {
						VisitExpression(*action->condition);
					}
					if (action->update_info) {
						VisitUpdateSet(*action->update_info);
					}
					VisitExpressions(action->expressions);
				}
			}
			VisitExpressions(merge.returning_list);
			break;
		}
		default:
			throw BinderException("%s does not support %s", REMOTE_QUERY_FUNCTION, EnumUtil::ToString(node.type));
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(
		    node, [&](unique_ptr<ParsedExpression> &expr) { VisitExpression(*expr); });
	}

	bool IsCTE(const BaseTableRef &ref) const {
		return ref.GetQualifiedName().Path().size() == 1 &&
		       std::find(cte_scope.begin(), cte_scope.end(), ref.Table()) != cte_scope.end();
	}

	void VisitTableRef(TableRef &ref) {
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE: {
			auto &table = ref.Cast<BaseTableRef>();
			table_callback(table, IsCTE(table));
			break;
		}
		case TableReferenceType::JOIN: {
			auto &join = ref.Cast<JoinRef>();
			VisitTableRef(*join.left);
			VisitTableRef(*join.right);
			if (join.condition) {
				VisitExpression(*join.condition);
			}
			break;
		}
		case TableReferenceType::SUBQUERY:
			VisitNode(*ref.Cast<SubqueryRef>().subquery->node);
			break;
		case TableReferenceType::EXPRESSION_LIST:
			for (auto &row : ref.Cast<ExpressionListRef>().values) {
				for (auto &expr : row) {
					VisitExpression(*expr);
				}
			}
			break;
		case TableReferenceType::EMPTY_FROM:
			break;
		default:
			throw BinderException("%s does not support table reference type %s", REMOTE_QUERY_FUNCTION,
			                      EnumUtil::ToString(ref.type));
		}
	}

	void VisitExpression(ParsedExpression &expr) {
		if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
			VisitNode(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node);
		}
		if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
			function_callback(expr.Cast<FunctionExpression>());
		}
		ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { VisitExpression(child); });
	}

	void VisitExpressions(vector<unique_ptr<ParsedExpression>> &expressions) {
		for (auto &expr : expressions) {
			VisitExpression(*expr);
		}
	}

	void VisitUpdateSet(UpdateSetInfo &set_info) {
		VisitExpressions(set_info.expressions);
		if (set_info.condition) {
			VisitExpression(*set_info.condition);
		}
	}

	std::function<void(BaseTableRef &, bool is_cte)> table_callback;
	std::function<void(FunctionExpression &)> function_callback;
	// The names of the CTEs in scope, innermost last.
	vector<Identifier> cte_scope;
};

bool HasEntry(ClientContext &context, CatalogType type, QualifiedName name) {
	EntryLookupInfo lookup(type, std::move(name));
	return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL) != nullptr;
}

QualifiedName QualifyInCatalog(const Identifier &catalog, const QualifiedName &name) {
	vector<Identifier> qualification {catalog};
	auto &path = name.Path();
	qualification.insert(qualification.end(), path.begin(), path.end() - 1);
	return QualifiedName(std::move(qualification), name.Name());
}

// The server resolves unqualified tables and functions in its default schema, so they are qualified with the schema
// the client resolves them to. CTE references and functions outside the catalog, such as built-ins, stay unqualified.
void QualifyNamesForServer(ClientContext &context, Catalog &catalog, QueryNode &node) {
	auto schemas = ClientData::Get(context).catalog_search_path->GetSchemasForCatalog(catalog.GetName());
	auto qualify = [&](CatalogType type, const QualifiedName &name) -> optional_ptr<const Identifier> {
		if (name.Path().size() != 1) {
			return nullptr;
		}
		for (auto &schema : schemas) {
			if (HasEntry(context, type, QualifiedName(catalog.GetName(), schema, name.Name()))) {
				return &schema;
			}
		}
		return nullptr;
	};
	PushedQueryVisitor visitor(
	    [&](BaseTableRef &ref, bool is_cte) {
		    if (is_cte) {
			    return;
		    }
		    auto schema = qualify(CatalogType::TABLE_ENTRY, ref.GetQualifiedName());
		    if (schema) {
			    ref.SetQualifiedName(Identifier(), *schema, ref.Table());
		    }
	    },
	    [&](FunctionExpression &function) {
		    auto schema = qualify(CatalogType::SCALAR_FUNCTION_ENTRY, function.GetQualifiedName());
		    if (schema) {
			    function.SetQualifiedName(Identifier(), *schema, function.FunctionName());
		    }
	    });
	visitor.VisitNode(node);
}

// Qualifies the tables and functions of a query sent to the server with `catalog`, so the client binds it to the same
// entries. Functions the catalog does not have, such as built-ins, stay unqualified.
void QualifyNamesForClient(ClientContext &context, Catalog &catalog, QueryNode &node) {
	const auto &catalog_name = catalog.GetName();
	PushedQueryVisitor visitor(
	    [&](BaseTableRef &ref, bool is_cte) {
		    if (is_cte) {
			    return;
		    }
		    auto &name = ref.GetQualifiedName();
		    if (name.Path().size() == 1) {
			    ref.SetQualifiedName(catalog_name, Identifier::DefaultSchema(), ref.Table());
			    return;
		    }
		    ref.SetQualifiedName(QualifyInCatalog(catalog_name, name));
	    },
	    [&](FunctionExpression &function) {
		    auto &name = function.GetQualifiedName();
		    auto qualified = name.Path().size() == 1
		                         ? QualifiedName(catalog_name, Identifier::DefaultSchema(), name.Name())
		                         : QualifyInCatalog(catalog_name, name);
		    if (HasEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, qualified)) {
			    function.SetQualifiedName(std::move(qualified));
		    }
	    });
	visitor.VisitNode(node);
}

struct RemoteQueryBindData : public TableFunctionData {
	RemoteQueryBindData(Identifier catalog_name_p, string sql_p, vector<LogicalType> types_p)
	    : catalog_name(std::move(catalog_name_p)), sql(std::move(sql_p)), types(std::move(types_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<RemoteQueryBindData>(catalog_name, sql, types);
		result->write_type = write_type;
		result->create_info = create_info ? create_info->Copy() : nullptr;
		result->estimated_cardinality = estimated_cardinality;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RemoteQueryBindData>();
		return catalog_name == other.catalog_name && sql == other.sql && types == other.types &&
		       write_type == other.write_type;
	}

	bool IsWrite() const {
		return write_type != StatementType::INVALID_STATEMENT;
	}

	Identifier catalog_name;
	string sql;
	// The result types as the client binds the query, which restore types Arrow cannot carry, such as ENUM.
	vector<LogicalType> types;
	// Set for statements writing to the database, which run as a statement on the server rather than as a query.
	StatementType write_type = StatementType::INVALID_STATEMENT;
	// The table a CREATE TABLE AS creates, cached on the client once the server created it.
	unique_ptr<CreateInfo> create_info;
	// Estimated from the plan the client binds, which reads the row counts of the remote tables.
	optional_idx estimated_cardinality;
};

struct RemoteQueryGlobalState : public GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return MaxValue<idx_t>(batches.size(), 1);
	}

	// The whole remote result of a query, fetched once. Each batch is converted by the thread that claims it.
	vector<std::shared_ptr<arrow::RecordBatch>> batches;
	atomic<idx_t> next_batch {0};
	// The result of a write, read by a single thread.
	unique_ptr<QueryResult> write_result;
};

struct RemoteQueryLocalState : public LocalTableFunctionState {
	unique_ptr<ArrowRecordBatchConverter> batch;
	idx_t batch_index = 0;
	// The last vector converted from batch, or fetched from the write result, which the output references.
	DataChunk chunk;
	unique_ptr<DataChunk> write_chunk;
};

// Rethrows the error of a statement the server failed to run, keeping its type when the server reported one.
void ThrowRemoteError(QueryResult &result, const char *operation) {
	auto &error = result.GetErrorObject();
	if (error.Type() == ExceptionType::INVALID) {
		throw IOException("Failed to %s on control node: %s", operation, error.RawMessage());
	}
	error.Throw();
}

QueryNode *GetStatementNode(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		return statement.Cast<SelectStatement>().node.get();
	case StatementType::INSERT_STATEMENT:
		return statement.Cast<InsertStatement>().node.get();
	case StatementType::DELETE_STATEMENT:
		return statement.Cast<DeleteStatement>().node.get();
	case StatementType::UPDATE_STATEMENT:
		return statement.Cast<UpdateStatement>().node.get();
	case StatementType::MERGE_INTO_STATEMENT:
		return statement.Cast<MergeIntoStatement>().node.get();
	default:
		return nullptr;
	}
}

bool IsDMLNode(QueryNodeType type) {
	switch (type) {
	case QueryNodeType::INSERT_QUERY_NODE:
	case QueryNodeType::UPDATE_QUERY_NODE:
	case QueryNodeType::DELETE_QUERY_NODE:
	case QueryNodeType::MERGE_QUERY_NODE:
		return true;
	default:
		return false;
	}
}

// DuckDB only allows data-modifying CTEs at the top level of a statement.
bool WritesData(const QueryNode &node) {
	if (IsDMLNode(node.type)) {
		return true;
	}
	for (auto &cte : node.cte_map.map) {
		if (cte.second->query_node && IsDMLNode(cte.second->query_node->type)) {
			return true;
		}
	}
	return false;
}

// A statement bound on its own binder still reads and writes the databases on behalf of the statement it is part of.
void AddStatementProperties(ClientContext &context, optional_ptr<Binder> binder, const StatementProperties &bound) {
	if (!binder) {
		return;
	}
	auto &properties = binder->GetStatementProperties();
	for (auto &entry : bound.read_databases) {
		properties.RegisterDBRead(Catalog::GetCatalog(context, entry.first), context);
	}
	for (auto &entry : bound.modified_databases) {
		properties.RegisterDBModify(Catalog::GetCatalog(context, entry.first), context, entry.second.modifications);
	}
	properties.return_type = bound.return_type;
}

unique_ptr<LogicalOperator> MakeRemoteGet(TableFunctionBindInput &input, TableIndex bind_index,
                                          unique_ptr<RemoteQueryBindData> bind_data, const vector<Identifier> &names) {
	auto types = bind_data->types;
	auto result = make_uniq<LogicalGet>(bind_index, input.table_function, std::move(bind_data), types, names);
	vector<ColumnIndex> column_ids;
	for (idx_t idx = 0; idx < types.size(); ++idx) {
		column_ids.emplace_back(idx);
	}
	result->SetColumnIds(std::move(column_ids));
	return std::move(result);
}

// Binds a CREATE TABLE AS on the client, which resolves the columns of the table to cache once the server created it.
unique_ptr<LogicalOperator> BindRemoteCreateTableAs(ClientContext &context, TableFunctionBindInput &input,
                                                    TableIndex bind_index, vector<Identifier> &return_names,
                                                    Catalog &catalog, CreateStatement &statement, string sql) {
	if (!SupportsStatementPushdown(statement)) {
		throw BinderException("%s only supports CREATE TABLE AS", REMOTE_QUERY_FUNCTION);
	}
	auto &info = statement.info->Cast<CreateTableInfo>();
	info.SetQualifiedName(QualifyInCatalog(catalog.GetName(), info.GetQualifiedName()));
	QualifyNamesForClient(context, catalog, *info.query->node);
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(std::move(statement.info));
	auto &properties = binder->GetStatementProperties();
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	AddStatementProperties(context, input.binder, properties);
	return_names = {Identifier("Count")};

	auto bind_data = make_uniq<RemoteQueryBindData>(catalog.GetName(), std::move(sql),
	                                                vector<LogicalType> {LogicalType::BIGINT});
	bind_data->write_type = StatementType::CREATE_STATEMENT;
	bind_data->create_info = bound_info->Base().Copy();
	bind_data->create_info->SetSchema(bound_info->schema.name);
	return MakeRemoteGet(input, bind_index, std::move(bind_data), return_names);
}

// Binds the query on the client, which checks it only reads tables the client knows and resolves its result schema.
// The query runs on the client instead if its result could differ on the server.
unique_ptr<LogicalOperator> RemoteQueryBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                    TableIndex bind_index, vector<Identifier> &return_names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a database name and a query", REMOTE_QUERY_FUNCTION);
	}
	Identifier catalog_name(StringValue::Get(input.inputs[0]));
	auto sql = StringValue::Get(input.inputs[1]);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	if (catalog.GetCatalogType() != "duckherder") {
		throw BinderException("%s requires a Duckherder database, but %s is not one", REMOTE_QUERY_FUNCTION,
		                      catalog_name);
	}

	Parser parser(context);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw BinderException("%s requires a single statement", REMOTE_QUERY_FUNCTION);
	}
	auto &statement = *parser.statements[0];
	if (statement.type == StatementType::CREATE_STATEMENT) {
		return BindRemoteCreateTableAs(context, input, bind_index, return_names, catalog,
		                               statement.Cast<CreateStatement>(), std::move(sql));
	}
	auto node = GetStatementNode(statement);
	if (!node) {
		throw BinderException("%s requires a query, a DML statement or CREATE TABLE AS", REMOTE_QUERY_FUNCTION);
	}
	QualifyNamesForClient(context, catalog, *node);
	if (WritesData(*node)) {
		// Data-modifying CTEs can only be bound by a binder without a parent.
		auto binder = Binder::CreateBinder(context);
		auto bound = binder->Bind(*node);
		AddStatementProperties(context, input.binder, binder->GetStatementProperties());
		QueryResult::DeduplicateColumns(bound.names);
		return_names = bound.names;
		auto bind_data = make_uniq<RemoteQueryBindData>(std::move(catalog_name), std::move(sql), bound.types);
		bind_data->write_type = statement.type;
		return MakeRemoteGet(input, bind_index, std::move(bind_data), return_names);
	}

	auto binder = Binder::CreateBinder(context, input.binder);
	auto bound = binder->Bind(*node);
	// A table function binding requires distinct column names, which a query result need not have.
	QueryResult::DeduplicateColumns(bound.names);
	return_names = bound.names;

	if (!QueryPushdownEnabled(context) || !HasDefaultQuerySettings(context) || DependsOnTimeZone(*bound.plan)) {
		auto bindings = bound.plan->GetColumnBindings();
		D_ASSERT(bindings.size() == bound.types.size());
		vector<unique_ptr<Expression>> select_list;
		for (idx_t idx = 0; idx < bindings.size(); ++idx) {
			select_list.push_back(
			    make_uniq<BoundColumnRefExpression>(bound.names[idx], bound.types[idx], bindings[idx]));
		}
		auto projection = make_uniq<LogicalProjection>(bind_index, std::move(select_list));
		projection->AddChild(std::move(bound.plan));
		return std::move(projection);
	}

	auto bind_data = make_uniq<RemoteQueryBindData>(std::move(catalog_name), std::move(sql), bound.types);
	bind_data->estimated_cardinality = bound.plan->EstimateCardinality(context);
	return MakeRemoteGet(input, bind_index, std::move(bind_data), return_names);
}

// Caches the table a CREATE TABLE AS created on the server, like the catalog loader caches the tables of the server.
void CreateLocalTable(ClientContext &context, Catalog &catalog, const CreateInfo &create_info) {
	auto &schema =
	    catalog.GetSchema(context, create_info.GetQualifiedName().Schema()).Cast<DuckherderSchemaCatalogEntry>();
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(create_info.Copy(), schema);
	schema.CreateTableLocal(catalog.GetCatalogTransaction(context), *bound_info);
}

unique_ptr<GlobalTableFunctionState> RemoteQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RemoteQueryBindData>();
	auto result = make_uniq<RemoteQueryGlobalState>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	auto &client = GetDistributedClient(context, catalog);
	if (bind_data.IsWrite()) {
		// The SQL names entries as the server does, so the server must not strip any catalog from it.
		result->write_result = client.ExecuteStatement(bind_data.sql, bind_data.write_type, Identifier(), &bind_data.types);
		if (result->write_result->HasError()) {
			ThrowRemoteError(*result->write_result, bind_data.create_info ? "execute CREATE TABLE AS" : "execute DML");
		}
		if (bind_data.create_info) {
			CreateLocalTable(context, catalog, *bind_data.create_info);
		}
		return std::move(result);
	}
	auto status = client.QueryBatches(bind_data.sql, result->batches);
	if (!status.ok()) {
		throw IOException("Remote query error: %s", status.ToString());
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> RemoteQueryInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	return make_uniq<RemoteQueryLocalState>();
}

void RemoteQueryExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RemoteQueryBindData>();
	auto &global_state = data.global_state->Cast<RemoteQueryGlobalState>();
	auto &local_state = data.local_state->Cast<RemoteQueryLocalState>();

	if (global_state.write_result) {
		local_state.write_chunk = global_state.write_result->Fetch();
		if (!local_state.write_chunk) {
			output.SetChildCardinality(0);
			return;
		}
		output.SetChildCardinality(local_state.write_chunk->size());
		for (idx_t col_idx = 0; col_idx < output.ColumnCount(); ++col_idx) {
			output.data[col_idx].Reference(local_state.write_chunk->data[col_idx]);
		}
		return;
	}
	while (!local_state.batch || !local_state.batch->HasNext()) {
		auto batch_idx = global_state.next_batch++;
		if (batch_idx >= global_state.batches.size()) {
			output.SetChildCardinality(0);
			return;
		}
		auto batch = std::move(global_state.batches[batch_idx]);
		local_state.batch = make_uniq<ArrowRecordBatchConverter>(context, *batch, &bind_data.types);
		local_state.batch_index = batch_idx;
	}
	if (local_state.chunk.ColumnCount() == 0) {
		local_state.chunk.Initialize(context, bind_data.types);
	}
	local_state.batch->Next(local_state.chunk);
	output.SetChildCardinality(local_state.chunk.size());
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); ++col_idx) {
		output.data[col_idx].Reference(local_state.chunk.data[col_idx]);
	}
}

unique_ptr<NodeStatistics> RemoteQueryCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RemoteQueryBindData>();
	if (!bind_data.estimated_cardinality.IsValid()) {
		return nullptr;
	}
	auto estimate = bind_data.estimated_cardinality.GetIndex();
	return make_uniq<NodeStatistics>(estimate, estimate);
}

// Batch indexes let DuckDB preserve the remote result order, e.g. of a pushed-down ORDER BY, across threads.
OperatorPartitionData RemoteQueryGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	return OperatorPartitionData(input.local_state->Cast<RemoteQueryLocalState>().batch_index);
}

InsertionOrderPreservingMap<string> RemoteQueryToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<RemoteQueryBindData>();
	result["Database"] = bind_data.catalog_name.GetIdentifierName();
	result["Query"] = bind_data.sql;
	return result;
}

void RemoteQuerySerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                          const BoundTableFunction &function) {
	auto &data = bind_data->Cast<RemoteQueryBindData>();
	serializer.WriteProperty(100, "catalog", data.catalog_name);
	serializer.WriteProperty(101, "sql", data.sql);
	serializer.WriteProperty(102, "types", data.types);
	serializer.WritePropertyWithDefault<uint8_t>(103, "write_type", static_cast<uint8_t>(data.write_type),
	                                             static_cast<uint8_t>(StatementType::INVALID_STATEMENT));
	serializer.WritePropertyWithDefault(104, "create_info", data.create_info);
}

unique_ptr<FunctionData> RemoteQueryDeserialize(Deserializer &deserializer, BoundTableFunction &function) {
	auto catalog_name = deserializer.ReadProperty<Identifier>(100, "catalog");
	auto sql = deserializer.ReadProperty<string>(101, "sql");
	auto types = deserializer.ReadProperty<vector<LogicalType>>(102, "types");
	auto result = make_uniq<RemoteQueryBindData>(std::move(catalog_name), std::move(sql), std::move(types));
	result->write_type = static_cast<StatementType>(deserializer.ReadPropertyWithExplicitDefault<uint8_t>(
	    103, "write_type", static_cast<uint8_t>(StatementType::INVALID_STATEMENT)));
	result->create_info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(104, "create_info");
	return std::move(result);
}

// Runs SQL of a connection CONNECT-ed to a Duckherder database on its server as is. The result schema is only known
// once the server ran the SQL, so binding runs it. Only CONNECT routes SQL here, and it binds each statement once.
unique_ptr<LogicalOperator> ConnectedQueryBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                       TableIndex bind_index, vector<Identifier> &return_names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a database name and a query", CONNECTED_QUERY_FUNCTION);
	}
	Identifier catalog_name(StringValue::Get(input.inputs[0]));
	auto sql = StringValue::Get(input.inputs[1]);
	auto connected = context.TryGetConnectedCatalog();
	if (!connected || connected->GetName() != catalog_name) {
		throw BinderException("%s only runs the SQL of a connection CONNECT-ed to %s", CONNECTED_QUERY_FUNCTION,
		                      catalog_name);
	}
	auto &catalog = connected->GetCatalog();

	// The parser of a CONNECT-ed connection passes all SQL through, and the server may parse SQL the client cannot,
	// e.g. of extensions only loaded on the server.
	unique_ptr<SQLStatement> statement;
	try {
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery(sql);
		if (parser.statements.size() == 1) {
			statement = std::move(parser.statements[0]);
		}
	} catch (std::exception &) { // NOLINT
	}
	auto statement_type = statement ? statement->type : StatementType::SELECT_STATEMENT;
	// Transaction statements run on the client transaction, which runs the server transaction. Otherwise an error would
	// abort the client transaction with no way to roll it back, and DISCONNECT would leave a server transaction open.
	if (statement_type == StatementType::TRANSACTION_STATEMENT) {
		auto info = std::move(statement->Cast<TransactionStatement>().info);
		if (input.binder) {
			auto &properties = input.binder->GetStatementProperties();
			properties.requires_valid_transaction = info->type == TransactionType::BEGIN_TRANSACTION;
			properties.return_type = StatementReturnType::NOTHING;
		}
		vector<unique_ptr<Expression>> select_list;
		select_list.push_back(make_uniq<BoundConstantExpression>(Value(LogicalType::BOOLEAN)));
		auto projection = make_uniq<LogicalProjection>(bind_index, std::move(select_list));
		projection->AddChild(make_uniq<LogicalTransaction>(std::move(info)));
		return_names = {Identifier("Success")};
		return std::move(projection);
	}
	// Starting the database's transaction runs the SQL in the server transaction of an explicit client transaction.
	Transaction::Get(context, catalog);
	auto result = GetDistributedClient(context, catalog).ExecuteStatement(sql, statement_type, Identifier());
	if (result->HasError()) {
		ThrowRemoteError(*result, "execute SQL");
	}
	return_names = result->GetNames();
	QueryResult::DeduplicateColumns(return_names);
	return make_uniq<LogicalColumnDataGet>(bind_index, result->GetTypes(), result->TakeCollection());
}

} // namespace

bool SupportsQueryPushdown(const ParsedExpression &expression) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::PARAMETER:
		// Prepared statement parameters are only bound on the client.
		return false;
	case ExpressionClass::FUNCTION:
		return !ReadsSessionState(expression.Cast<FunctionExpression>().FunctionName());
	case ExpressionClass::CAST:
		return IsBuiltinType(expression.Cast<CastExpression>().TargetType());
	case ExpressionClass::TYPE:
		return IsBuiltinType(expression.Cast<TypeExpression>());
	default:
		return true;
	}
}

bool SupportsQueryPushdown(const TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::JOIN:
	case TableReferenceType::SUBQUERY:
	case TableReferenceType::EXPRESSION_LIST:
	case TableReferenceType::EMPTY_FROM:
		return true;
	default:
		// Such as table functions or SUMMARIZE, which may name tables without table references, or client data.
		return false;
	}
}

bool SupportsQueryPushdown(const QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
	case QueryNodeType::SET_OPERATION_NODE:
	case QueryNodeType::RECURSIVE_CTE_NODE:
	case QueryNodeType::INSERT_QUERY_NODE:
	case QueryNodeType::UPDATE_QUERY_NODE:
	case QueryNodeType::DELETE_QUERY_NODE:
	case QueryNodeType::MERGE_QUERY_NODE:
		return true;
	default:
		return false;
	}
}

bool SupportsStatementPushdown(const SQLStatement &statement) {
	if (statement.type != StatementType::CREATE_STATEMENT) {
		// Other DDL goes through the catalog, which caches the created entries on the client.
		return false;
	}
	auto &info = *statement.Cast<CreateStatement>().info;
	return info.type == CatalogType::TABLE_ENTRY && info.Cast<CreateTableInfo>().query && !info.temporary;
}

namespace {

unique_ptr<TableRef> CreateFunctionRef(const char *function_name, Catalog &catalog, const string &sql) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(Literal::String(catalog.GetName().GetIdentifierName())));
	arguments.push_back(make_uniq<ConstantExpression>(Literal::String(sql)));
	auto result = make_uniq<TableFunctionRef>();
	// Qualified, so a macro of the same name on the search path cannot replace it.
	result->function = make_uniq<FunctionExpression>(
	    QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), Identifier(function_name)),
	    std::move(arguments));
	return std::move(result);
}

} // namespace

unique_ptr<TableRef> CreateRemoteQueryRef(ClientContext &context, Catalog &catalog, unique_ptr<QueryNode> node) {
	QualifyNamesForServer(context, catalog, *node);
	return CreateFunctionRef(REMOTE_QUERY_FUNCTION, catalog, node->ToString());
}

unique_ptr<TableRef> CreateRemoteStatementRef(ClientContext &context, Catalog &catalog,
                                              unique_ptr<SQLStatement> statement) {
	if (!SupportsStatementPushdown(*statement)) {
		throw InternalException("%s cannot run a %s statement", REMOTE_QUERY_FUNCTION,
		                        StatementTypeToString(statement->type));
	}
	auto &info = statement->Cast<CreateStatement>().info->Cast<CreateTableInfo>();
	// The server would create the table in its default schema rather than the one the client picks.
	auto &name = info.GetQualifiedName();
	if (name.Schema().empty()) {
		auto schema = ClientData::Get(context).catalog_search_path->GetDefaultSchema(context, catalog.GetName());
		if (!schema) {
			throw BinderException("Catalog \"%s\" has no default schema - specify a schema explicitly",
			                      catalog.GetName());
		}
		info.SetQualifiedName(QualifiedName(Identifier(), *schema, name.Name()));
	}
	QualifyNamesForServer(context, catalog, *info.query->node);
	return CreateFunctionRef(REMOTE_QUERY_FUNCTION, catalog, statement->ToString());
}

unique_ptr<TableRef> CreateConnectedQueryRef(Catalog &catalog, const string &sql) {
	return CreateFunctionRef(CONNECTED_QUERY_FUNCTION, catalog, sql);
}

TableFunction GetRemoteQueryFunction() {
	TableFunction function(REMOTE_QUERY_FUNCTION, {LogicalType::VARCHAR, LogicalType::VARCHAR}, RemoteQueryExecute,
	                       nullptr, RemoteQueryInitGlobal, RemoteQueryInitLocal);
	function.bind_operator = RemoteQueryBindOperator;
	function.cardinality = RemoteQueryCardinality;
	function.get_partition_data = RemoteQueryGetPartitionData;
	function.to_string = RemoteQueryToString;
	function.serialize = RemoteQuerySerialize;
	function.deserialize = RemoteQueryDeserialize;
	return function;
}

TableFunction GetConnectedQueryFunction() {
	TableFunction function(CONNECTED_QUERY_FUNCTION, {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr);
	function.bind_operator = ConnectedQueryBindOperator;
	return function;
}

} // namespace duckdb
