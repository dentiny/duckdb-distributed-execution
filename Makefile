PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckherder
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

DUCKDB_REMOTE_STATEMENT_PATCH=$(PROJ_DIR)patches/duckdb-v1.5.5-remote-statement.patch

.PHONY: prepare-duckdb-remote-statement
prepare-duckdb-remote-statement:
	@if git -C duckdb apply --reverse --check "$(DUCKDB_REMOTE_STATEMENT_PATCH)" >/dev/null 2>&1; then \
		:; \
	else \
		git -C duckdb apply --check "$(DUCKDB_REMOTE_STATEMENT_PATCH)" && \
		git -C duckdb apply "$(DUCKDB_REMOTE_STATEMENT_PATCH)"; \
	fi

clangd debug release relassert reldebug test test_release test_debug test_reldebug tidy-check wasm_mvp wasm_eh \
wasm_threads extension_configuration_default extension_configuration: prepare-duckdb-remote-statement

test_release:
	ctest --test-dir build/release/extension/duckherder --output-on-failure

test_debug:
	ctest --test-dir build/debug/extension/duckherder --output-on-failure

test_reldebug:
	ctest --test-dir build/reldebug/extension/duckherder --output-on-failure

format-all: format
	find unit/ -iname *.hpp -o -iname *.cpp | xargs /usr/bin/clang-format --sort-includes=0 -style=file -i
	@cmake-format -i CMakeLists.txt
	@cmake-format -i test/unittest/CMakeLists.txt
	@buf format -w src/proto/

test-object-storage-s3:
	bash test/object_storage/run_single_writer_reader_e2e.sh

.PHONY: format-all test-object-storage-s3
