#!/bin/bash

echo "🔄 Waiting for vcpkg to finish installing Arrow and dependencies..."
echo "Started at: $(date)"

# Wait for vcpkg to finish
while ps aux | grep -q "[v]cpkg install"; do
    sleep 30
    echo "$(date '+%H:%M:%S') - Still installing packages..."
done

echo ""
echo "✅ vcpkg installation completed at: $(date)"
echo ""

# Check what was installed
echo "📦 Installed packages:"
cd /home/vscode/duckdb-distributed-execution
/home/vscode/vcpkg/vcpkg list 2>&1 | grep -E "(arrow|grpc|protobuf)" || echo "Key packages not found"
echo ""

# Try to build
echo "🔨 Building extension..."
export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake
cd /home/vscode/duckdb-distributed-execution

OVERRIDE_GIT_DESCRIBE=v1.4.1 CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make debug > /tmp/build.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ BUILD SUCCESSFUL!"
    echo ""
    
    # Run tests
    echo "🧪 Running tests..."
    make test_debug > /tmp/test.log 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ TESTS PASSED!"
        echo ""
        echo "📊 Test Summary:"
        tail -20 /tmp/test.log
    else
        echo "❌ TESTS FAILED"
        echo ""
        echo "📊 Test Errors:"
        tail -50 /tmp/test.log
    fi
else
    echo "❌ BUILD FAILED"
    echo ""
    echo "📊 Build Errors:"
    tail -100 /tmp/build.log
fi

echo ""
echo "Completed at: $(date)"
echo ""
echo "📄 Full logs available at:"
echo "  - Build: /tmp/build.log"
echo "  - Test: /tmp/test.log"

