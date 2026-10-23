## 0.1.0

### Added

- Add `duckherder_query(database, query)` to run a SELECT query on the server of an attached Duckherder database.

### Fixed

- Use an explicit row-count type when batching Arrow results on Linux and update object storage to the merged DuckDB v2 statistics and diagnostic fixes.
- Use current vector APIs for exchange batches and declare network/server scalar operations fallible on DuckDB v2.

### Changed

- Push whole queries to the server through DuckDB's remote catalog pushdown, which also pushes the remote parts of
  set operations, `CREATE TABLE AS` and `INSERT ... SELECT` into local tables, and folds client-side constants first.
- Upgrade DuckDB to `v2.0.0`, extension-ci-tools to the latest commit, and the embedded object-storage extension to its v2-compatible `0.2.0` release.
- Require C++17 and use constexpr string views for fixed configuration, cache, and protocol names.
- Adapt catalog, DDL, query planning, result schemas, and serialization to DuckDB's Identifier and QualifiedName APIs.
- Use v2 QueryResult, retained collections, and QueryResultStream for Arrow output and worker streaming.
- Reuse DuckDB's annotated mutex/lock types and SQL/ALTER rendering instead of local implementations.
- Update static registration, CI workflow pins, and extension release metadata.

## 0.0.10

### Added

- Add descriptions, examples, categories, and argument names for all Duckherder SQL and pragma functions in
  `duckdb_functions()`.

### Changed

- Update DuckDB and extension-ci-tools to `v1.5.6` and synchronize the embedded duckdb-object-storage dependency.

- Use the Duckherder `ATTACH` path as the remote `host:port` endpoint instead of `server_host` and `server_port`
  options.

### Removed

- Remove the public `duckherder_register_remote_table` pragma; remote tables are registered automatically during
  discovery and creation.

### Fixed

- Load core functions in embedded server and worker databases so the loadable extension can attach and execute queries.

- Keep Duckling and Duckherder transaction state in their registered managers so unique indexes include existing rows.

- Fix static linking of the distributed server, worker, and unit tests against DuckDB and its extension loader.

- Fix extension version ([#122])

[#122]: https://github.com/dentiny/duckdb-distributed-execution/pull/122

## 0.0.9

### Changed

- Update duckdb and extension-ci-tools to v1.5.5

## 0.0.8

### Changed

- Update duckdb and extension-ci-tools to v1.5.0

## 0.0.7

### Fixed

- Update extension-ci-tools to v1.4.4

## 0.0.6

### Changed

- Update duckdb to v1.4.4

## 0.0.5

### Changed

- Update duckdb and extension-ci-tools to v1.4.3

## 0.0.3

### Added

- Provide distributed execution stats, including distribution mode, query execution wall-clock time, number of tasks spawned, etc ([#89])

[#89]: https://github.com/dentiny/duckdb-distributed-execution/pull/89

- Provide executable for driver node and worker nodes, and scalar function to register and replace ([#88], [#94])

[#88]: https://github.com/dentiny/duckdb-distributed-execution/pull/88

[#94]: https://github.com/dentiny/duckdb-distributed-execution/pull/94
