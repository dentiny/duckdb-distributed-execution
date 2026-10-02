#include "client/execution/distributed_query_pushdown.hpp"

#include "client/execution/distributed_table_scan_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

// Results on TIMESTAMPTZ and TIMETZ may depend on client settings such as TimeZone, which the server lacks.
bool DependsOnTimeZone(const LogicalType &type) {
	return type.id() == LogicalTypeId::TIMESTAMP_TZ || type.id() == LogicalTypeId::TIME_TZ;
}

bool DependsOnTimeZone(const Expression &expr) {
	if (DependsOnTimeZone(expr.return_type)) {
		return true;
	}
	bool depends = false;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { depends = depends || DependsOnTimeZone(child); });
	return depends;
}

// The server runs queries with default settings, so the client must not have changed any affecting query results.
bool HasDefaultQuerySettings(ClientContext &context) {
	return Settings::Get<DefaultOrderSetting>(context) == OrderType::ASCENDING &&
	       Settings::Get<DefaultNullOrderSetting>(context) == DefaultOrderByNullType::NULLS_LAST &&
	       Settings::Get<DefaultCollationSetting>(context).empty() && !Settings::Get<IntegerDivisionSetting>(context) &&
	       Settings::Get<IeeeFloatingPointOpsSetting>(context) &&
	       Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(context);
}

// Functions reading the session state, environment or sequences, which differ on the server.
bool ReadsSessionState(const string &function_name) {
	static const case_insensitive_set_t SESSION_FUNCTIONS {
	    "currval",        "current_catalog",   "current_connection_id",  "current_database",
	    "current_date",   "current_localtime", "current_localtimestamp", "current_query",
	    "current_schema", "current_schemas",   "current_setting",        "current_transaction_id",
	    "getenv",         "getvariable",       "in_search_path",         "nextval",
	    "today",          "txid_current"};
	return SESSION_FUNCTIONS.find(function_name) != SESSION_FUNCTIONS.end();
}

// Returns whether every table read by `op` is a remote table of the same database as `scan`, which is set to one of
// the remote scans.
bool ReadsOnlyRemoteTables(LogicalOperator &op, optional_ptr<LogicalGet> &scan) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.function.name != "distributed_scan") {
			return false;
		}
		auto &bind_data = get.bind_data->Cast<DistributedTableScanBindData>();
		if (scan && &scan->bind_data->Cast<DistributedTableScanBindData>().table.ParentCatalog() !=
		                &bind_data.table.ParentCatalog()) {
			return false;
		}
		// Filters pushed into the scan no longer appear as expressions.
		for (auto &entry : get.table_filters.filters) {
			LogicalType type;
			GetRemoteColumn(bind_data, entry.first, type);
			if (DependsOnTimeZone(type)) {
				return false;
			}
		}
		scan = &get;
	}
	bool depends = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expr) { depends = depends || DependsOnTimeZone(**expr); });
	if (depends) {
		return false;
	}
	for (auto &child : op.children) {
		if (!ReadsOnlyRemoteTables(*child, scan)) {
			return false;
		}
	}
	return true;
}

// Rewrites a parsed query so the server resolves it to the same tables and functions as the client.
class RemoteQueryRewriter {
public:
	RemoteQueryRewriter(ClientContext &context_p, Catalog &catalog_p) : context(context_p), catalog(catalog_p) {
	}

	// Returns false if the query references anything the server cannot resolve identically.
	bool Rewrite(QueryNode &node) {
		VisitNode(node);
		return pushable;
	}

private:
	void VisitNode(QueryNode &node) {
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    node, [&](unique_ptr<ParsedExpression> &expr) { VisitExpression(*expr); },
		    [&](TableRef &ref) { VisitTableRef(ref); });
	}

	void VisitExpression(ParsedExpression &expr) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::CAST: {
			auto &type = expr.Cast<CastExpression>().cast_type;
			if (type.IsUnbound()) {
				VisitExpression(*UnboundType::GetTypeExpression(type));
			}
			break;
		}
		case ExpressionClass::TYPE: {
			// Client-side types are unknown to the server.
			auto &type = expr.Cast<TypeExpression>();
			pushable = pushable && type.GetCatalog().empty() && type.GetSchema().empty() &&
			           DefaultTypeGenerator::GetDefaultType(type.GetTypeName()) != LogicalTypeId::INVALID;
			break;
		}
		case ExpressionClass::COLUMN_REF: {
			// Table names are rewritten without the client's catalog, so columns qualified with it no longer resolve.
			auto &column_names = expr.Cast<ColumnRefExpression>().column_names;
			pushable =
			    pushable && !(column_names.size() > 2 && StringUtil::CIEquals(column_names[0], catalog.GetName()));
			break;
		}
		case ExpressionClass::FUNCTION: {
			// Client-side macros and functions are unknown to the server.
			auto &function = expr.Cast<FunctionExpression>();
			EntryLookupInfo lookup(CatalogType::SCALAR_FUNCTION_ENTRY, function.function_name);
			auto entry =
			    Catalog::GetEntry(context, function.catalog, function.schema, lookup, OnEntryNotFound::RETURN_NULL);
			pushable = pushable && entry && entry->ParentCatalog().IsSystemCatalog() &&
			           !ReadsSessionState(function.function_name);
			break;
		}
		case ExpressionClass::SUBQUERY:
			VisitNode(*expr.Cast<SubqueryExpression>().subquery->node);
			break;
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { VisitExpression(child); });
	}

	void VisitTableRef(TableRef &ref) {
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE:
			VisitBaseTable(ref.Cast<BaseTableRef>());
			break;
		case TableReferenceType::EMPTY_FROM:
		case TableReferenceType::EXPRESSION_LIST:
		case TableReferenceType::JOIN:
		case TableReferenceType::SUBQUERY:
			break;
		default:
			// Such as SUMMARIZE or table functions, which may name tables without table references.
			pushable = false;
			break;
		}
	}

	void VisitBaseTable(BaseTableRef &table_ref) {
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table_ref.table_name);
		auto entry = Catalog::GetEntry(context, table_ref.catalog_name, table_ref.schema_name, lookup,
		                               OnEntryNotFound::RETURN_NULL);
		// Like the binder, `a.b` falls back to table `b` in catalog `a` when there is no schema `a`.
		if (!entry && table_ref.catalog_name.empty() && !table_ref.schema_name.empty()) {
			entry = Catalog::GetEntry(context, table_ref.schema_name, "", lookup, OnEntryNotFound::RETURN_NULL);
		}
		// Views are only known to the client, and the client's catalog name is unknown to the server.
		const bool remote_table =
		    entry && entry->type == CatalogType::TABLE_ENTRY && &entry->ParentCatalog() == &catalog;
		if (table_ref.catalog_name.empty() && table_ref.schema_name.empty()) {
			// An unqualified name may refer to a CTE, so it is kept, which only resolves to the same table on the
			// server in its default schema. No table entry means a CTE, as the plan only reads remote tables.
			pushable = pushable && (!entry || (remote_table && entry->ParentSchema().name == DEFAULT_SCHEMA));
			return;
		}
		if (!remote_table) {
			pushable = false;
			return;
		}
		table_ref.catalog_name.clear();
		table_ref.schema_name = entry->ParentSchema().name;
		table_ref.table_name = entry->name;
	}

	ClientContext &context;
	Catalog &catalog;
	bool pushable = true;
};

void OptimizeDistributedQuery(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &context = input.context;
	Value enabled;
	context.TryGetCurrentSetting(QUERY_PUSHDOWN_SETTING, enabled);
	if (!enabled.GetValue<bool>() || !HasDefaultQuerySettings(context)) {
		return;
	}
	optional_ptr<LogicalGet> scan;
	if (!ReadsOnlyRemoteTables(*plan, scan) || !scan) {
		return;
	}

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(context.GetCurrentQuery());
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return;
	}
	auto &statement = parser.statements[0]->Cast<SelectStatement>();
	// Prepared statement parameters are only bound on the client.
	if (!statement.named_param_map.empty()) {
		return;
	}
	auto &bind_data = scan->bind_data->Cast<DistributedTableScanBindData>();
	RemoteQueryRewriter rewriter(context, bind_data.table.ParentCatalog());
	if (!rewriter.Rewrite(*statement.node)) {
		return;
	}

	plan->ResolveOperatorTypes();
	auto types = plan->types;
	vector<string> names;
	vector<ColumnIndex> column_ids;
	for (idx_t idx = 0; idx < types.size(); ++idx) {
		names.emplace_back(StringUtil::Format("#%llu", idx));
		column_ids.emplace_back(idx);
	}
	auto pushed_bind_data = unique_ptr_cast<FunctionData, DistributedTableScanBindData>(bind_data.Copy());
	pushed_bind_data->pushed_query = statement.ToString();
	pushed_bind_data->pushed_types = types;
	auto result = make_uniq<LogicalGet>(input.optimizer.binder.GenerateTableIndex(), scan->function,
	                                    std::move(pushed_bind_data), std::move(types), std::move(names));
	result->SetColumnIds(std::move(column_ids));
	plan = std::move(result);
}

} // namespace

OptimizerExtension GetDistributedQueryPushdownExtension() {
	OptimizerExtension extension;
	extension.optimize_function = OptimizeDistributedQuery;
	return extension;
}

} // namespace duckdb
