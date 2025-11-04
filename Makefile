PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckherder
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

format-all: format
	find unit/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	@cmake-format -i CMakeLists.txt
	@buf format -w src/proto/

test_unit: all
	find build/release/extension/duckherder/ -type f -name "test*" -not -name "*.o" -not -name "*.cpp" -not -name "*.d" -exec {} \;

test_reldebug_unit: reldebug
	find build/reldebug/extension/duckherder/ -type f -name "test*" -not -name "*.o" -not -name "*.cpp" -not -name "*.d" -exec {} \;

test_debug_unit: debug
	find build/debug/extension/duckherder/ -type f -name "test*" -not -name "*.o" -not -name "*.cpp" -not -name "*.d" -exec {} \;

.PHONY: format-all test_unit test_reldebug_unit test_debug_unit
