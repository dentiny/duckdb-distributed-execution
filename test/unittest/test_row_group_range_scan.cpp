#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "server/worker/row_group_range_scan.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Row group range scans start at the first row group of the range", "[row_group_range_scan]") {
	DuckDB db(nullptr);
	OptimizerExtension::Register(DBConfig::GetConfig(*db.instance), GetRowGroupRangeScanExtension());
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE t AS SELECT i AS id FROM range(5 * 122880) t(i)")->HasError());

	// Row groups 2 and 3, the range a worker task gets.
	auto plan = con.ExtractPlan("SELECT id FROM t WHERE rowid >= 245760 AND rowid < 491520");
	REQUIRE(plan != nullptr);
	auto *op = plan.get();
	while (op->children.size() == 1) {
		op = op->children[0].get();
	}
	REQUIRE(op->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = op->Cast<LogicalGet>();
	auto &column_ids = get.GetColumnIds();
	// The scan's filters are keyed by position in its columns.
	TableFilterSet filters;
	for (idx_t idx = 0; idx < column_ids.size(); ++idx) {
		if (column_ids[idx].IsRowIdColumn()) {
			filters.PushFilter(ColumnIndex(idx), get.table_filters.filters.at(COLUMN_IDENTIFIER_ROW_ID)->Copy());
		}
	}
	REQUIRE(filters.filters.size() == 1);

	con.context->RunFunctionInTransaction([&]() {
		TableFunctionInitInput input(get.bind_data.get(), column_ids, get.projection_ids, &filters);
		auto state = get.function.init_global(*con.context, input);
		// One thread per row group: the row group before the range, whose last rowid is 245759, is not read.
		REQUIRE(state->MaxThreads() == 2);
	});
}
