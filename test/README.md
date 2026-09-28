# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## Upstream DuckDB SQLLogicTests

Selected tests from DuckDB's own `duckdb/test/sql` tree can also run through a
Duckherder client attachment. The test configuration starts an in-process
Control Node, attaches it as the active catalog, and lets DuckDB's existing
SQLLogicTest runner compare the client-visible results with the expected
results in each upstream test.

Build the desired configuration first, then run the corresponding target:

```bash
make reldebug
make test_reldebug_duckdb
```

By default, this runs the complete SQLLogicTest suite registered by DuckDB.
Pass an upstream test path as a make argument to run a smaller workload:

```bash
make test_reldebug_duckdb "test/sql/function/operator/test_arithmetic_sqllogic.test"
```

The same entry point is available as `test_debug_duckdb` and
`test_release_duckdb`. `DUCKDB_TEST_FILTER` can also override the default Catch
filter directly.

This first integration step intentionally uses one driver-only Control Node,
one fixed local port, and one writable client connection. Tests that require
multiple writable connections, database reloads, or features not yet supported
by Duckherder are expected to need additional work before they can run through
this harness.