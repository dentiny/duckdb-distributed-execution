cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME motherduck)

set(CMAKE_CXX_STANDARD 20)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED CONFIG)
find_package(ArrowFlight REQUIRED CONFIG)
find_package(Protobuf REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(${ARROW_INCLUDE_DIR})

# Generate protobuf code
set(PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/distributed.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Add directory for generated protobuf headers
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(EXTENSION_SOURCES
    ${PROTO_SRCS} # Generated protobuf sources
    src/base_query_recorder.cpp
    src/distributed_delete.cpp
    src/distributed_flight_client.cpp
    src/distributed_flight_server.cpp
    src/distributed_insert.cpp
    src/distributed_server.cpp
    src/distributed_table_scan_function.cpp
    src/entry_lookup_info_hash_utils.cpp
    src/motherduck_catalog.cpp
    src/motherduck_extension.cpp
    src/motherduck_pragmas.cpp
    src/motherduck_storage.cpp
    src/motherduck_schema_catalog_entry.cpp
    src/motherduck_table_catalog_entry.cpp
    src/motherduck_transaction.cpp
    src/motherduck_transaction_manager.cpp
    src/query_history_query_function.cpp
    src/query_recorder.cpp
    src/query_recorder_factory.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link all required libraries Note: gRPC and Protobuf are transitive
# dependencies of ArrowFlight
target_link_libraries(
  ${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Arrow::arrow_static
  ArrowFlight::arrow_flight_static protobuf::libprotobuf)

target_link_libraries(
  ${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Arrow::arrow_static
  ArrowFlight::arrow_flight_static protobuf::libprotobuf)

# Add distributed server executable TODO: Fix duplicate symbol linking issue
# before enabling add_executable(distributed_server
# src/distributed_server_main.cpp) target_link_libraries(distributed_server
# ${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Arrow::arrow_static
# ArrowFlight::arrow_flight_static gRPC::grpc++ protobuf::libprotobuf )

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
