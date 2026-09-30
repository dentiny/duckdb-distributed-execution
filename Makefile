PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

ifeq ($(shell uname -s),Darwin)
MACOSX_DEPLOYMENT_TARGET ?= $(shell sw_vers -productVersion | awk -F. '{print $$1 ".0"}')
# DuckDB recommends disabling this sanitizer on Apple Silicon because it can
# report false positives.
EXT_FLAGS += -DDISABLE_VPTR_SANITIZER=1
# duckdb-object-storage still supports CMake versions older than 3.10.
EXT_FLAGS += -Wno-deprecated
EXT_FLAGS += -DCMAKE_EXE_LINKER_FLAGS=-Wl,-no_warn_duplicate_libraries
EXT_FLAGS += -DCMAKE_SHARED_LINKER_FLAGS=-Wl,-no_warn_duplicate_libraries
EXT_FLAGS += -DCMAKE_MODULE_LINKER_FLAGS=-Wl,-no_warn_duplicate_libraries
export MACOSX_DEPLOYMENT_TARGET
endif

# Configuration of extension
EXT_NAME=duckherder
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

DUCKHERDER_UNITTEST_RELEASE := ./build/release/extension/$(EXT_NAME)/test/unittest/unittest_$(EXT_NAME)
DUCKHERDER_UNITTEST_DEBUG := ./build/debug/extension/$(EXT_NAME)/test/unittest/unittest_$(EXT_NAME)
DUCKHERDER_UNITTEST_RELDEBUG := ./build/reldebug/extension/$(EXT_NAME)/test/unittest/unittest_$(EXT_NAME)

test_release_internal: test_duckherder_release test_duckherder_loadable_release
test_debug_internal: test_duckherder_debug test_duckherder_loadable_debug
test_reldebug_internal: test_duckherder_reldebug test_duckherder_loadable_reldebug

test_duckherder_release:
	$(DUCKHERDER_UNITTEST_RELEASE)

test_duckherder_debug:
	$(DUCKHERDER_UNITTEST_DEBUG)

test_duckherder_reldebug:
	$(DUCKHERDER_UNITTEST_RELDEBUG)

# Exercise the distributable artifact as well as the statically linked test runner.
.PHONY: test_duckherder_loadable_release test_duckherder_loadable_debug test_duckherder_loadable_reldebug
test_duckherder_loadable_release test_duckherder_loadable_debug test_duckherder_loadable_reldebug:
	./build/$(patsubst test_duckherder_loadable_%,%,$@)/test/unittest --test-config test/configs/loadable.json "test/*"

DUCKDB_TEST_TARGETS := test_debug_duckdb test_reldebug_duckdb test_release_duckdb \
	test_debug_duckdb_slow test_reldebug_duckdb_slow test_release_duckdb_slow
DUCKDB_TEST_ARGUMENTS := $(filter-out $(DUCKDB_TEST_TARGETS),$(MAKECMDGOALS))
DUCKDB_TEST_FILTER ?= $(DUCKDB_TEST_ARGUMENTS)
DUCKDB_TEST_EXCLUDE ?= ~*_slow
DUCKDB_TEST_CONFIG := test/configs/duckherder.json

ifneq ($(filter $(DUCKDB_TEST_TARGETS),$(MAKECMDGOALS)),)
ifneq ($(strip $(DUCKDB_TEST_ARGUMENTS)),)
.PHONY: $(DUCKDB_TEST_ARGUMENTS)
$(DUCKDB_TEST_ARGUMENTS):
	@:
endif
endif

define RUN_DUCKDB_TESTS
TEST_PID=; \
cleanup() { \
	if [ -n "$$TEST_PID" ]; then \
		rm -rf "$(PROJ_DIR)duckdb/duckdb_unittest_tempdir/$$TEST_PID"; \
	fi; \
}; \
terminate() { \
	if [ -n "$$TEST_PID" ]; then kill "$$TEST_PID" 2>/dev/null || true; fi; \
}; \
trap cleanup EXIT; \
trap terminate INT TERM; \
./build/$(1)/test/unittest --test-config $(DUCKDB_TEST_CONFIG) --test-dir duckdb \
	$(if $(DUCKDB_TEST_FILTER),"$(DUCKDB_TEST_FILTER)") \
	$(if $(DUCKDB_TEST_EXCLUDE),"$(DUCKDB_TEST_EXCLUDE)") & \
TEST_PID=$$!; \
wait "$$TEST_PID"
endef

test_debug_duckdb:
	@$(call RUN_DUCKDB_TESTS,debug)

test_debug_duckdb_slow: DUCKDB_TEST_FILTER := *_slow
test_debug_duckdb_slow: DUCKDB_TEST_EXCLUDE :=
test_debug_duckdb_slow:
	@$(call RUN_DUCKDB_TESTS,debug)

test_reldebug_duckdb:
	@$(call RUN_DUCKDB_TESTS,reldebug)

test_reldebug_duckdb_slow: DUCKDB_TEST_FILTER := *_slow
test_reldebug_duckdb_slow: DUCKDB_TEST_EXCLUDE :=
test_reldebug_duckdb_slow:
	@$(call RUN_DUCKDB_TESTS,reldebug)

test_release_duckdb:
	@$(call RUN_DUCKDB_TESTS,release)

test_release_duckdb_slow: DUCKDB_TEST_FILTER := *_slow
test_release_duckdb_slow: DUCKDB_TEST_EXCLUDE :=
test_release_duckdb_slow:
	@$(call RUN_DUCKDB_TESTS,release)

format-all: format
	clang-format --sort-includes=0 -style=file -i $(wildcard test/unittest/*.hpp test/unittest/*.cpp)
	@cmake-format -i CMakeLists.txt
	@cmake-format -i test/unittest/CMakeLists.txt
	@buf format -w --path src/proto

test-object-storage-s3:
	bash test/object_storage/run_single_writer_reader_e2e.sh

.PHONY: format-all test-object-storage-s3 test_duckherder_release test_duckherder_debug test_duckherder_reldebug \
	$(DUCKDB_TEST_TARGETS)
