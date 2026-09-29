cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME duckherder)

set(CMAKE_CXX_FLAGS"${CMAKE_CXX_FLAGS} -Wunused-variable -Wunused-function -Wunused-but-set-variable"
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=thread-safety")
endif()

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
set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/client.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/dml.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/query.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/transaction.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/worker.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/distributed.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Add directory for generated protobuf headers.
include_directories(${CMAKE_CURRENT_BINARY_DIR})

if(${BUILD_UNITTESTS})
  add_subdirectory(test/unittest)
endif()

set(UTIL_SOURCES
    src/utils/catalog_utils.cpp src/utils/network_utils.cpp
    src/utils/retry_utils.cpp src/utils/thread_utils.cpp
    src/utils/time_utils.cpp)

set(CLIENT_EXECUTION_SOURCES
    src/client/execution/distributed_alter_table.cpp
    src/client/execution/distributed_client.cpp
    src/client/execution/distributed_create_index.cpp
    src/client/execution/distributed_table_scan_function.cpp
    src/client/execution/logical_remote_alter_table.cpp
    src/client/execution/logical_remote_create_index.cpp
    src/client/execution/remote_dml.cpp)

set(CLIENT_TRANSPORT_SOURCES src/client/transport/distributed_flight_client.cpp
                             src/client/transport/transaction_state.cpp)

set(CLIENT_SOURCES
    ${CLIENT_EXECUTION_SOURCES}
    ${CLIENT_TRANSPORT_SOURCES}
    src/client/duckherder_catalog.cpp
    src/client/duckherder_connection_state.cpp
    src/client/duckherder_index_catalog_entry.cpp
    src/client/duckherder_pragmas.cpp
    src/client/duckherder_remote_endpoint.cpp
    src/client/duckherder_remote_table_config.cpp
    src/client/duckherder_storage.cpp
    src/client/duckherder_schema_catalog_entry.cpp
    src/client/duckherder_table_catalog_entry.cpp
    src/client/duckherder_transaction.cpp
    src/client/duckherder_transaction_manager.cpp
    src/client/entry_lookup_info_hash_utils.cpp)

set(SERVER_UTIL_SOURCES src/server/validation.cpp)

set(SERVER_DRIVER_SOURCES
    src/server/driver/client_registration.cpp
    src/server/driver/distributed_executor.cpp
    src/server/driver/distributed_flight_server.cpp
    src/server/driver/distributed_flight_server_test_state.cpp
    src/server/driver/distributed_server_function.cpp
    src/server/driver/distributed_server_main.cpp
    src/server/driver/duckling_storage.cpp
    src/server/driver/worker_node_client.cpp
    src/server/driver/duckling_transaction.cpp
    src/server/driver/duckling_transaction_manager.cpp
    src/server/driver/partition_sql_generator.cpp
    src/server/driver/plan_serializer.cpp
    src/server/driver/query_plan_analyzer.cpp
    src/server/driver/query_utils.cpp
    src/server/driver/result_merger.cpp
    src/server/driver/task_partitioner.cpp
    src/server/driver/worker_manager.cpp)

set(SERVER_WORKER_SOURCES src/server/worker/worker_node.cpp)

set(EXTENSION_SOURCES
    ${PROTO_SRCS} # Generated protobuf sources
    ${CLIENT_SOURCES}
    ${SERVER_UTIL_SOURCES}
    ${SERVER_DRIVER_SOURCES}
    ${SERVER_WORKER_SOURCES}
    ${UTIL_SOURCES}
    src/arrow_utils.cpp
    src/base_query_recorder.cpp
    src/duckherder_extension.cpp
    src/duckherder_functions.cpp
    src/duckherder_extension_instance_state.cpp
    src/query_execution_stats_query_function.cpp
    src/query_history_query_function.cpp
    src/query_recorder.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set_target_properties(
  ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME}
  PROPERTIES CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON
             CXX_EXTENSIONS OFF)

# Note: gRPC and Protobuf are transitive dependencies of ArrowFlight.
target_link_libraries(
  ${EXTENSION_NAME} Arrow::arrow_static ArrowFlight::arrow_flight_static
  protobuf::libprotobuf gRPC::grpc++)
set_property(
  TARGET ${EXTENSION_NAME}
  PROPERTY
    INTERFACE_LINK_LIBRARIES
    "duckdb_static;$<LINK_ONLY:Arrow::arrow_static>;$<LINK_ONLY:ArrowFlight::arrow_flight_static>;$<LINK_ONLY:protobuf::libprotobuf>;$<LINK_ONLY:gRPC::grpc++>"
)

target_link_libraries(
  ${LOADABLE_EXTENSION_NAME} Arrow::arrow_static
  ArrowFlight::arrow_flight_static protobuf::libprotobuf gRPC::grpc++)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# Link DuckDB before its generated extension loader, as the DuckDB shell does;
# static linkers need the core references before scanning the loader archive.
# Add distributed server executable.
add_executable(distributed_server src/server/driver/distributed_server_main.cpp)
target_link_libraries(
  distributed_server duckdb_static Arrow::arrow_static
  ArrowFlight::arrow_flight_static protobuf::libprotobuf gRPC::grpc++)
link_extension_libraries(distributed_server "")

# Add distributed worker executable.
add_executable(distributed_worker src/server/worker/distributed_worker_main.cpp)
target_link_libraries(
  distributed_worker duckdb_static Arrow::arrow_static
  ArrowFlight::arrow_flight_static protobuf::libprotobuf gRPC::grpc++)
link_extension_libraries(distributed_worker "")
