PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=motherduck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Formatting target
.PHONY: format-all

format-all:
	@echo "🎨 Formatting CMake files..."
	@cmake-format -i CMakeLists.txt || echo "⚠️  cmake-format not installed. Install with: pip install cmake-format"
	@echo "🎨 Formatting protobuf files..."
	@buf format -w src/proto/
	@echo "✅ All formatting complete!"