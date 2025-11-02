# vcpkg Setup for Arrow Flight Dependencies

## What We've Done

Based on the [DuckDB dependency management guide](https://duckdb.org/2024/03/22/dependency-management), we've set up vcpkg to manage the Arrow Flight dependencies for distributed execution.

### Steps Completed

1. **Installed vcpkg**
   ```bash
   cd /home/vscode
   git clone https://github.com/Microsoft/vcpkg.git
   cd vcpkg
   ./bootstrap-vcpkg.sh -disableMetrics
   ```

2. **Set VCPKG_TOOLCHAIN_PATH**
   ```bash
   export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake
   ```
   
   This has been added to `~/.bashrc` for persistence.

3. **Created vcpkg.json**
   The dependencies are specified in `/home/vscode/duckdb-distributed-execution/vcpkg.json`:
   ```json
   {
     "dependencies": [
       { "name": "openssl" },
       {
         "name": "arrow",
         "default-features": false,
         "features": ["filesystem", "flight", "parquet"]
       },
       { "name": "grpc" },
       { "name": "protobuf" }
     ]
   }
   ```

4. **Installing Dependencies** (Currently Running)
   ```bash
   vcpkg install --triplet=arm64-linux
   ```
   
   This is installing 87 packages including:
   - Arrow (with Flight, filesystem, parquet features)
   - gRPC
   - Protobuf
   - All Boost dependencies
   - OpenSSL
   - Various compression libraries (zlib, zstd, brotli, lz4, etc.)

## How vcpkg Integration Works

When you run `make debug` with `VCPKG_TOOLCHAIN_PATH` set, DuckDB's build system:

1. Detects vcpkg via the toolchain file
2. Reads `vcpkg.json` for dependencies
3. Installs any missing packages
4. Makes them available to CMake via `find_package()`
5. Links them automatically to your extension

## Build Commands

Once vcpkg finishes installing dependencies:

```bash
# Set the toolchain path (already in ~/.bashrc)
export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build in debug mode
cd /home/vscode/duckdb-distributed-execution
OVERRIDE_GIT_DESCRIBE=v1.4.1 CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make debug

# Run tests
make test_debug
```

## CMakeLists.txt Configuration

The `CMakeLists.txt` has been configured to use these dependencies:

```cmake
# Find packages (vcpkg makes these available)
find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED)
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

# Link to extension
target_link_libraries(${EXTENSION_NAME} 
  OpenSSL::SSL 
  OpenSSL::Crypto
  Arrow::arrow_shared
  gRPC::grpc++
  protobuf::libprotobuf
)
```

## Current Status

✅ vcpkg installed and bootstrapped  
✅ VCPKG_TOOLCHAIN_PATH configured  
✅ vcpkg.json created with Arrow Flight dependencies  
🔄 **Currently installing 87 packages** (this takes 10-30 minutes)  
⏳ Ready to build once installation completes  

## Checking Installation Progress

To check if vcpkg is done installing:

```bash
ps aux | grep vcpkg
```

When there's no vcpkg process running, installation is complete.

## After Installation

Once vcpkg finishes, you can build the extension:

```bash
cd /home/vscode/duckdb-distributed-execution
export VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake
OVERRIDE_GIT_DESCRIBE=v1.4.1 CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make debug
```

The build will:
1. Find all Arrow/gRPC/Protobuf packages via vcpkg
2. Compile the distributed execution extension
3. Link Arrow Flight for RPC communication
4. Create the `distributed_server` executable

## Troubleshooting

### If vcpkg installation fails

Check the error and try:
```bash
cd /home/vscode/duckdb-distributed-execution
/home/vscode/vcpkg/vcpkg install --triplet=arm64-linux --debug
```

### If build still can't find Arrow

Ensure VCPKG_TOOLCHAIN_PATH is set:
```bash
echo $VCPKG_TOOLCHAIN_PATH
# Should output: /home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### To clean and rebuild

```bash
cd /home/vscode/duckdb-distributed-execution
rm -rf build/
OVERRIDE_GIT_DESCRIBE=v1.4.1 CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make debug
```

## Next Steps After Successful Build

1. Update `PhysicalDistributedInsert` to use Arrow RecordBatch instead of SQL strings
2. Update `DistributedTableScanFunction` to use Flight client
3. Test the Flight server standalone
4. Test end-to-end distributed execution

See `DISTRIBUTED_EXECUTION_ARCHITECTURE.md` for implementation details.

