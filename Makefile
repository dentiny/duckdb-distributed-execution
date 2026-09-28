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

format-all: format
	clang-format --sort-includes=0 -style=file -i $(wildcard test/unittest/*.hpp test/unittest/*.cpp)
	@cmake-format -i CMakeLists.txt
	@cmake-format -i test/unittest/CMakeLists.txt
	@buf format -w --path src/proto

test-object-storage-s3:
	bash test/object_storage/run_single_writer_reader_e2e.sh

.PHONY: format-all test-object-storage-s3
