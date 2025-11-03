cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME motherduck)

set(CMAKE_CXX_STANDARD 20)

find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED CONFIG)
find_package(ArrowFlight REQUIRED CONFIG)
find_package(Protobuf REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(src/include/client)
include_directories(src/include/server)
include_directories(${ARROW_INCLUDE_DIR})

# Generate protobuf code.
set(PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/distributed.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Add directory for generated protobuf headers.
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(UTIL_SOURCES src/utils/time_utils.cpp)

set(CLIENT_SOURCES
    src/client/distributed_alter_table.cpp
    src/client/distributed_client.cpp
    src/client/distributed_create_index.cpp
    src/client/distributed_delete.cpp
    src/client/distributed_flight_client.cpp
    src/client/distributed_insert.cpp
    src/client/distributed_table_scan_function.cpp
    src/client/entry_lookup_info_hash_utils.cpp
    src/client/logical_remote_alter_table.cpp
    src/client/logical_remote_create_index.cpp
    src/client/motherduck_catalog.cpp
    src/client/motherduck_index_catalog_entry.cpp
    src/client/motherduck_pragmas.cpp
    src/client/motherduck_storage.cpp
    src/client/motherduck_schema_catalog_entry.cpp
    src/client/motherduck_table_catalog_entry.cpp
    src/client/motherduck_transaction.cpp
    src/client/motherduck_transaction_manager.cpp)

set(SERVER_SOURCES src/server/distributed_flight_server.cpp
                   src/server/distributed_server_function.cpp)

set(EXTENSION_SOURCES
    ${PROTO_SRCS} # Generated protobuf sources
    ${CLIENT_SOURCES}
    ${SERVER_SOURCES}
    ${UTIL_SOURCES}
    src/arrow_utils.cpp
    src/base_query_recorder.cpp
    src/query_history_query_function.cpp
    src/query_recorder.cpp
    src/query_recorder_factory.cpp
    src/motherduck_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Note: gRPC and Protobuf are transitive dependencies of ArrowFlight.
target_link_libraries(
  ${EXTENSION_NAME}
  OpenSSL::SSL
  OpenSSL::Crypto
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc)

target_link_libraries(
  ${LOADABLE_EXTENSION_NAME}
  OpenSSL::SSL
  OpenSSL::Crypto
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# Add distributed server executable.
add_executable(distributed_server src/server/distributed_server_main.cpp)
target_link_libraries(
  distributed_server
  ${EXTENSION_NAME}
  OpenSSL::SSL
  OpenSSL::Crypto
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc)

# Unit tests.
include_directories(duckdb/third_party/catch)
add_executable(test_distributed_flight unit/test_distributed_flight.cpp)
target_link_libraries(
  test_distributed_flight
  ${EXTENSION_NAME}
  OpenSSL::SSL
  OpenSSL::Crypto
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc)
