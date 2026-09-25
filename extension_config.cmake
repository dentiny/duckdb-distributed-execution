# This file is included by DuckDB's build system. It specifies which extension
# to load

# Extension from this repo
duckdb_extension_load(duckherder SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION 0.0.9 LOAD_TESTS)

# Build the object-storage filesystem from the pinned submodule so the same
# DuckDB binary can attach databases through duckdb_objfs://.
duckdb_extension_load(
  duckdb_object_storage SOURCE_DIR
  ${CMAKE_CURRENT_LIST_DIR}/duckdb-object-storage LOAD_TESTS)
