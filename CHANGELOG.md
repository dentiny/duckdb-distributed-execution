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
