PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckherder
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

format-all: format
	find unit/ -iname *.hpp -o -iname *.cpp | xargs /usr/bin/clang-format --sort-includes=0 -style=file -i
	@cmake-format -i CMakeLists.txt
	@cmake-format -i test/unittest/CMakeLists.txt
	@buf format -w src/proto/

test-object-storage-s3:
	bash test/object_storage/run_single_writer_reader_e2e.sh

.PHONY: format-all test-object-storage-s3
