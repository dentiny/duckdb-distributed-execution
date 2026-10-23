# This file is included by DuckDB's build system. It specifies which extension
# to load

# Extension from this repo
duckdb_extension_load(duckherder SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION 0.1.0 LOAD_TESTS)
duckdb_extension_statically_link(duckherder duckdb_object_storage)

# Build the object-storage filesystem from the pinned submodule so the same
# DuckDB binary can attach databases through duckdb_objfs://.
duckdb_extension_load(
  duckdb_object_storage SOURCE_DIR
  ${CMAKE_CURRENT_LIST_DIR}/duckdb-object-storage EXTENSION_VERSION 0.2.0
  LOAD_TESTS)

# Optional: build latency_injection_fs from the checkout in
# LATENCY_INJECTION_FS_DIR.
if(NOT "$ENV{LATENCY_INJECTION_FS_DIR}" STREQUAL "")
  duckdb_extension_load(latency_injection_fs SOURCE_DIR
                        $ENV{LATENCY_INJECTION_FS_DIR})
  duckdb_extension_statically_link(latency_injection_fs)
endif()
