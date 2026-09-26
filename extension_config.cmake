# This file is included by DuckDB's build system. It specifies which extension
# to load

# Extension from this repo
duckdb_extension_load(duckherder SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION 0.0.9 LOAD_TESTS)

# Build the object-storage filesystem from the pinned submodule so the same
# DuckDB binary can attach databases through duckdb_objfs://.
set(OBJECT_STORAGE_IMMUTABLE_SOURCE_DIR
    ${CMAKE_CURRENT_LIST_DIR}/duckdb-object-storage)
set(OBJECT_STORAGE_COMPAT_PATCH
    ${CMAKE_CURRENT_LIST_DIR}/patches/duckdb-object-storage-ca15f79.patch)
set(OBJECT_STORAGE_COMMIT ed06b778c68a567cc08ed9d70d9cb029813f57ac)
execute_process(
  COMMAND git -C ${OBJECT_STORAGE_IMMUTABLE_SOURCE_DIR} rev-parse HEAD
  OUTPUT_VARIABLE OBJECT_STORAGE_ACTUAL_COMMIT
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE OBJECT_STORAGE_REVISION_RESULT)
if(NOT OBJECT_STORAGE_REVISION_RESULT EQUAL 0
   OR NOT OBJECT_STORAGE_ACTUAL_COMMIT STREQUAL OBJECT_STORAGE_COMMIT)
  message(FATAL_ERROR
          "duckdb-object-storage must be pinned at ${OBJECT_STORAGE_COMMIT}")
endif()

file(SHA256 ${OBJECT_STORAGE_COMPAT_PATCH} OBJECT_STORAGE_PATCH_HASH)
set(OBJECT_STORAGE_SOURCE_DIR
    ${CMAKE_BINARY_DIR}/duckdb-object-storage-${OBJECT_STORAGE_COMMIT}-${OBJECT_STORAGE_PATCH_HASH})
if(NOT EXISTS ${OBJECT_STORAGE_SOURCE_DIR}/.duckherder-patched)
  file(REMOVE_RECURSE ${OBJECT_STORAGE_SOURCE_DIR})
  execute_process(
    COMMAND git clone --quiet --no-checkout
            ${OBJECT_STORAGE_IMMUTABLE_SOURCE_DIR} ${OBJECT_STORAGE_SOURCE_DIR}
    RESULT_VARIABLE OBJECT_STORAGE_CLONE_RESULT)
  if(NOT OBJECT_STORAGE_CLONE_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to copy duckdb-object-storage into the build tree")
  endif()
  execute_process(
    COMMAND git -C ${OBJECT_STORAGE_SOURCE_DIR} checkout --quiet --detach
            ${OBJECT_STORAGE_COMMIT}
    RESULT_VARIABLE OBJECT_STORAGE_CHECKOUT_RESULT)
  if(NOT OBJECT_STORAGE_CHECKOUT_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to check out the pinned duckdb-object-storage")
  endif()
  execute_process(
    COMMAND git -C ${OBJECT_STORAGE_SOURCE_DIR} apply --check
            ${OBJECT_STORAGE_COMPAT_PATCH}
    RESULT_VARIABLE OBJECT_STORAGE_PATCH_CHECK_RESULT)
  if(NOT OBJECT_STORAGE_PATCH_CHECK_RESULT EQUAL 0)
    message(FATAL_ERROR "duckdb-object-storage compatibility patch does not apply")
  endif()
  execute_process(
    COMMAND git -C ${OBJECT_STORAGE_SOURCE_DIR} apply
            ${OBJECT_STORAGE_COMPAT_PATCH}
    RESULT_VARIABLE OBJECT_STORAGE_PATCH_RESULT)
  if(NOT OBJECT_STORAGE_PATCH_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to patch the build-tree duckdb-object-storage")
  endif()
  file(WRITE ${OBJECT_STORAGE_SOURCE_DIR}/.duckherder-patched
       "${OBJECT_STORAGE_COMMIT}\n${OBJECT_STORAGE_PATCH_HASH}\n")
endif()
duckdb_extension_load(
  duckdb_object_storage SOURCE_DIR ${OBJECT_STORAGE_SOURCE_DIR} LOAD_TESTS)
