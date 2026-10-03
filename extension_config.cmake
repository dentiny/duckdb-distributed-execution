# This file is included by DuckDB's build system. It specifies which extension
# to load

# Extension from this repo
duckdb_extension_load(duckherder SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION 0.0.10 LOAD_TESTS)

# Build the object-storage filesystem from the pinned submodule so the same
# DuckDB binary can attach databases through duckdb_objfs://.
duckdb_extension_load(
  duckdb_object_storage SOURCE_DIR
  ${CMAKE_CURRENT_LIST_DIR}/duckdb-object-storage LOAD_TESTS)

# Benchmark builds only: set LATENCY_INJECTION_FS_DIR to a
# duckdb-filesystem-latency-injection checkout to simulate storage latency
# through DUCKHERDER_STARTUP_SQL.
if(DEFINED ENV{LATENCY_INJECTION_FS_DIR})
  duckdb_extension_load(latency_injection_fs SOURCE_DIR
                        $ENV{LATENCY_INJECTION_FS_DIR})
endif()
